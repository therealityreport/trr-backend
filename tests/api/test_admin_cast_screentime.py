from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.auth import require_cast_screentime_admin
from api.main import app
from api.routers import admin_cast_screentime as router_module
from trr_backend.repositories import cast_screentime as repo
from trr_backend.services import cast_screentime_artifacts, retained_cast_screentime_dispatch


@pytest.fixture(autouse=True)
def override_admin(request):
    if "no_admin_override" in request.fixturenames:
        yield
        return
    app.dependency_overrides[require_cast_screentime_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_cast_screentime_admin, None)


@pytest.fixture(autouse=True)
def set_service_token(monkeypatch):
    monkeypatch.setenv("SCREENALYTICS_SERVICE_TOKEN", "test-token")
    yield


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def no_admin_override():
    return True


@pytest.fixture(autouse=True)
def fake_repo(monkeypatch):
    store: dict[str, dict] = {
        "sessions": {},
        "video_assets": {},
        "runs": {},
        "segments": {},
        "evidence": {},
        "excluded": {},
        "metrics": {},
        "artifacts": {},
        "publish_versions": {},
        "reference_fingerprints": {},
        "suggestion_decisions": {},
        "unknown_review_state": {},
    }

    def create_media_upload_session(payload):
        upload_session_id = str(payload.get("id") or uuid4())
        record = {**payload, "id": upload_session_id}
        store["sessions"][upload_session_id] = record
        return record

    def get_media_upload_session(upload_session_id):
        return store["sessions"].get(upload_session_id)

    def update_media_upload_session(upload_session_id, payload):
        row = store["sessions"].get(upload_session_id)
        if not row:
            return None
        row.update(payload)
        return row

    def create_video_asset(payload):
        record = {
            "legacy_screenalytics_video_asset_id": None,
            **payload,
        }
        store["video_assets"][record["id"]] = record
        return record

    def get_video_asset(video_asset_id):
        return store["video_assets"].get(video_asset_id)

    def get_video_asset_by_legacy_screenalytics_id(legacy_video_asset_id):
        return next(
            (
                row
                for row in store["video_assets"].values()
                if row.get("legacy_screenalytics_video_asset_id") == legacy_video_asset_id
            ),
            None,
        )

    def resolve_video_asset(video_asset_id):
        return get_video_asset(video_asset_id) or get_video_asset_by_legacy_screenalytics_id(video_asset_id)

    def get_video_asset_upload_session_status(video_asset_id):
        for session in store["sessions"].values():
            if session.get("promoted_video_asset_id") == video_asset_id:
                return session.get("status")
        return None

    def list_video_asset_cast_candidates(video_asset_id):
        if video_asset_id not in store["video_assets"]:
            return []
        person_id = str(uuid4())
        return [
            {
                "person_id": person_id,
                "display_name": "Test Person",
                "source": "manual",
                "confidence": 1.0,
                "credit_category": "cast",
                "billing_order": 1,
                "role": "Self",
            }
        ]

    def resolve_owner_context(*, owner_scope=None, owner_id=None, show_id=None, season_id=None, episode_id=None):
        if owner_id and owner_scope == "show":
            return {
                "owner_scope": "show",
                "owner_id": owner_id,
                "show_id": owner_id,
                "season_id": None,
                "episode_id": None,
                "show_name": "Test Show",
            }
        if owner_id and owner_scope == "season":
            return {
                "owner_scope": "season",
                "owner_id": owner_id,
                "show_id": str(show_id or uuid4()),
                "season_id": owner_id,
                "episode_id": None,
                "show_name": "Test Show",
            }
        if owner_id and owner_scope == "episode":
            resolved_season_id = str(season_id or uuid4())
            resolved_show_id = str(show_id or uuid4())
            return {
                "owner_scope": "episode",
                "owner_id": owner_id,
                "show_id": resolved_show_id,
                "season_id": resolved_season_id,
                "episode_id": owner_id,
                "show_name": "Test Show",
            }
        if episode_id:
            return {
                "owner_scope": "episode",
                "owner_id": episode_id,
                "show_id": str(show_id or uuid4()),
                "season_id": str(season_id or uuid4()),
                "episode_id": episode_id,
                "show_name": "Test Show",
            }
        if season_id:
            return {
                "owner_scope": "season",
                "owner_id": season_id,
                "show_id": str(show_id or uuid4()),
                "season_id": season_id,
                "episode_id": None,
                "show_name": "Test Show",
            }
        if show_id:
            return {
                "owner_scope": "show",
                "owner_id": show_id,
                "show_id": show_id,
                "season_id": None,
                "episode_id": None,
                "show_name": "Test Show",
            }
        return None

    def list_target_youtube_accounts(*, show_id, season_id=None):
        return [{"show_id": show_id, "season_id": season_id, "account_handle": "bravo", "config": {}}]

    def get_social_youtube_video(youtube_video_row_id):
        return {
            "id": youtube_video_row_id,
            "video_id": "abc123def45",
            "channel_id": "",
            "channel_title": "Bravo",
            "source_account": "bravo",
            "show_id": str(uuid4()),
            "season_id": str(uuid4()),
            "hosted_media_urls": ["https://cdn.example.com/social/youtube/test/trailer.mp4"],
            "raw_data": {"uploader": "Bravo", "uploader_url": "https://www.youtube.com/@bravo"},
        }

    def list_candidate_cast_snapshot(*, video_asset_id=None, show_id=None, season_id=None, episode_id=None):
        lookup_id = video_asset_id or show_id or season_id or episode_id
        if not lookup_id:
            return []
        return list_video_asset_cast_candidates(
            next(iter(store["video_assets"])) if store["video_assets"] else lookup_id
        )

    def build_candidate_cast_snapshot(
        *, video_asset_id=None, show_id=None, season_id=None, episode_id=None, media_type=None, owner_scope=None
    ):
        snapshot = list_candidate_cast_snapshot(
            video_asset_id=video_asset_id,
            show_id=show_id,
            season_id=season_id,
            episode_id=episode_id,
        )
        return {
            "snapshot": snapshot,
            "candidate_scope_policy": {
                "media_type": media_type or "episode",
                "owner_scope": owner_scope,
                "primary_scope": owner_scope or "show",
                "scope_order": [owner_scope] if owner_scope else ["show"],
                "fallback_scopes_used": [],
                "preferred_facebank_coverage": True,
            },
            "cast_coverage_summary": {
                "candidate_count": len(snapshot),
                "approved_facebank_coverage_count": len(snapshot),
                "fallback_scopes_used": [],
                "warning": None,
                "warnings": [],
            },
        }

    def create_run(payload):
        run_id = str(uuid4())
        record = {
            "id": run_id,
            "status": payload.get("status", "pending"),
            "video_asset_id": payload["video_asset_id"],
            "run_type": payload.get("run_type", "cast_screentime"),
            "pipeline_version": payload.get("pipeline_version"),
            "execution_backend": payload.get("execution_backend"),
            "review_status": payload.get("review_status", "draft"),
            "run_config_json": payload.get("run_config_json", {}),
            "config_hash": payload.get("config_hash"),
            "candidate_cast_snapshot_json": payload.get("candidate_cast_snapshot_json", []),
            "candidate_scope_policy_json": payload.get("candidate_scope_policy_json", {}),
            "cast_coverage_summary_json": payload.get("cast_coverage_summary_json", {}),
            "dispatch_status": payload.get("dispatch_status"),
            "dispatch_job_id": payload.get("dispatch_job_id"),
            "dispatch_accepted_at": payload.get("dispatch_accepted_at"),
            "manifest_key": None,
            "error_message": None,
            "started_at": None,
            "completed_at": None,
            "worker_heartbeat_at": None,
            "effective_runtime_seconds": None,
        }
        store["runs"][run_id] = record
        return record

    def get_run(run_id):
        return store["runs"].get(run_id)

    def get_run_with_video_asset(run_id):
        run = store["runs"].get(run_id)
        if not run:
            return None
        asset = store["video_assets"].get(run["video_asset_id"], {})
        return {**asset, **run}

    def update_run(run_id, payload):
        run = store["runs"].get(run_id)
        if not run:
            return None
        run.update(payload)
        return run

    def upsert_cast_screentime_evidence(run_id, items):
        rows = []
        bucket = store["evidence"].setdefault(run_id, [])
        by_key = {row["evidence_key"]: row for row in bucket}
        for item in items:
            existing = by_key.get(item["evidence_key"])
            if existing:
                existing.update(item)
                rows.append(existing)
            else:
                record = {**item, "run_id": run_id}
                bucket.append(record)
                by_key[item["evidence_key"]] = record
                rows.append(record)
        return rows

    def replace_segments(run_id, items):
        rows = [{**item, "run_id": run_id} for item in items]
        store["segments"][run_id] = rows
        return rows

    def replace_evidence(run_id, items):
        rows = [{**item, "run_id": run_id} for item in items]
        store["evidence"][run_id] = rows
        return rows

    def replace_excluded(run_id, items):
        rows = [{**item, "run_id": run_id} for item in items]
        store["excluded"][run_id] = rows
        return rows

    def replace_metrics(run_id, items):
        rows = [{**item, "run_id": run_id} for item in items]
        store["metrics"][run_id] = rows
        return rows

    def reconcile_stale_runs(*, stale_after_seconds, show_id=None):
        reconciled = []
        for run in store["runs"].values():
            if run.get("status") != "running":
                continue
            asset = store["video_assets"].get(run.get("video_asset_id"), {})
            if show_id and str(asset.get("show_id") or "") != str(show_id):
                continue
            run.update({"status": "failed", "error_message": "worker_heartbeat_expired"})
            reconciled.append(run)
        return reconciled

    def get_publish_version_for_run(run_id):
        return next((row for row in store["publish_versions"].values() if row["run_id"] == run_id), None)

    def publish_run(*, run_id, video_asset_id, published_by, notes_json, metrics_snapshot_json):
        existing = get_publish_version_for_run(run_id)
        if existing:
            return existing
        for row in store["publish_versions"].values():
            if row["video_asset_id"] == video_asset_id and row.get("is_current"):
                row["is_current"] = False
        version_number = (
            max(
                (
                    int(row["version_number"])
                    for row in store["publish_versions"].values()
                    if row["video_asset_id"] == video_asset_id
                ),
                default=0,
            )
            + 1
        )
        record = {
            "id": str(uuid4()),
            "video_asset_id": video_asset_id,
            "run_id": run_id,
            "version_number": version_number,
            "published_by": published_by,
            "published_at": "2026-03-16T12:00:00+00:00",
            "notes_json": notes_json,
            "metrics_snapshot_json": metrics_snapshot_json,
            "is_current": True,
        }
        store["publish_versions"][record["id"]] = record
        return record

    def list_publish_versions(video_asset_id):
        asset = store["video_assets"].get(video_asset_id, {})
        rows = [row for row in store["publish_versions"].values() if row["video_asset_id"] == video_asset_id]
        rows.sort(key=lambda item: int(item["version_number"]), reverse=True)
        return [
            {
                **row,
                "media_type": asset.get("media_type"),
                "media_kind": asset.get("media_kind"),
                "video_class": asset.get("video_class"),
                "promo_subtype": asset.get("promo_subtype"),
                "show_id": asset.get("show_id"),
                "season_id": asset.get("season_id"),
                "episode_id": asset.get("episode_id"),
                "run_status": store["runs"].get(row["run_id"], {}).get("status"),
                "review_status": store["runs"].get(row["run_id"], {}).get("review_status"),
                "effective_runtime_seconds": store["runs"].get(row["run_id"], {}).get("effective_runtime_seconds"),
            }
            for row in rows
        ]

    def replace_reference_fingerprints_for_run(*, run_id, show_id, season_id, episode_id, video_asset_id, fingerprints):
        rows = [
            {
                **item,
                "id": str(uuid4()),
                "run_id": run_id,
                "show_id": show_id,
                "season_id": season_id,
                "episode_id": episode_id,
                "video_asset_id": video_asset_id,
            }
            for item in fingerprints
        ]
        store["reference_fingerprints"][run_id] = rows
        return rows

    def list_current_published_versions_for_show(show_id):
        rows = []
        for row in store["publish_versions"].values():
            if not row.get("is_current"):
                continue
            asset = store["video_assets"].get(row["video_asset_id"], {})
            if asset.get("show_id") != show_id or asset.get("media_type") != "episode":
                continue
            rows.append({**row, **asset})
        return rows

    def list_current_published_versions_for_season(season_id):
        rows = []
        for row in store["publish_versions"].values():
            if not row.get("is_current"):
                continue
            asset = store["video_assets"].get(row["video_asset_id"], {})
            if asset.get("season_id") != season_id or asset.get("media_type") != "episode":
                continue
            rows.append({**row, **asset})
        return rows

    def upsert_suggestion_decision(payload):
        key = (payload["owner_scope"], payload["owner_entity_id"], payload["person_id"])
        record = {
            "id": str(uuid4()),
            **payload,
            "decided_at": "2026-03-16T12:34:56+00:00",
        }
        store["suggestion_decisions"][key] = record
        return record

    def list_suggestion_decisions_for_context(*, show_id, season_id=None, episode_id=None):
        rows = []
        for row in store["suggestion_decisions"].values():
            if row.get("show_id") != show_id:
                continue
            if row.get("owner_scope") == "season" and season_id and row.get("owner_entity_id") == season_id:
                rows.append(row)
            elif row.get("owner_scope") == "episode" and episode_id and row.get("owner_entity_id") == episode_id:
                rows.append(row)
            elif row.get("owner_scope") == "show" and row.get("owner_entity_id") == show_id:
                rows.append(row)
        return rows

    def upsert_unknown_review_state(payload):
        key = (payload["owner_scope"], payload["owner_entity_id"], payload["queue_group"])
        record = {
            "id": str(uuid4()),
            **payload,
            "decided_at": "2026-03-16T12:34:56+00:00",
        }
        store["unknown_review_state"][key] = record
        return record

    def list_unknown_review_state_for_context(*, show_id, season_id=None, episode_id=None):
        rows = []
        for row in store["unknown_review_state"].values():
            if row.get("show_id") != show_id:
                continue
            if row.get("owner_scope") == "season" and season_id and row.get("owner_entity_id") == season_id:
                rows.append(row)
            elif row.get("owner_scope") == "episode" and episode_id and row.get("owner_entity_id") == episode_id:
                rows.append(row)
            elif row.get("owner_scope") == "show" and row.get("owner_entity_id") == show_id:
                rows.append(row)
        return rows

    monkeypatch.setattr(repo, "create_media_upload_session", create_media_upload_session)
    monkeypatch.setattr(repo, "get_media_upload_session", get_media_upload_session)
    monkeypatch.setattr(repo, "update_media_upload_session", update_media_upload_session)
    monkeypatch.setattr(repo, "create_video_asset", create_video_asset)
    monkeypatch.setattr(repo, "get_video_asset", get_video_asset)
    monkeypatch.setattr(repo, "get_video_asset_by_legacy_screenalytics_id", get_video_asset_by_legacy_screenalytics_id)
    monkeypatch.setattr(repo, "resolve_video_asset", resolve_video_asset)
    monkeypatch.setattr(repo, "get_video_asset_upload_session_status", get_video_asset_upload_session_status)
    monkeypatch.setattr(repo, "list_video_asset_cast_candidates", list_video_asset_cast_candidates)
    monkeypatch.setattr(repo, "resolve_owner_context", resolve_owner_context)
    monkeypatch.setattr(repo, "list_target_youtube_accounts", list_target_youtube_accounts)
    monkeypatch.setattr(repo, "get_social_youtube_video", get_social_youtube_video)
    monkeypatch.setattr(repo, "list_candidate_cast_snapshot", list_candidate_cast_snapshot)
    monkeypatch.setattr(repo, "build_candidate_cast_snapshot", build_candidate_cast_snapshot)
    monkeypatch.setattr(repo, "create_run", create_run)
    monkeypatch.setattr(repo, "get_run", get_run)
    monkeypatch.setattr(repo, "get_run_with_video_asset", get_run_with_video_asset)
    monkeypatch.setattr(repo, "update_run", update_run)

    def upsert_run_artifacts(run_id, artifacts):
        rows = [{**item, "run_id": run_id} for item in artifacts]
        store["artifacts"][run_id] = rows
        return rows

    monkeypatch.setattr(repo, "upsert_run_artifacts", upsert_run_artifacts)
    monkeypatch.setattr(
        repo,
        "get_run_artifact",
        lambda run_id, artifact_key: next(
            (item for item in store["artifacts"].get(run_id, []) if item["artifact_key"] == artifact_key), None
        ),
    )
    monkeypatch.setattr(repo, "replace_cast_screentime_segments", replace_segments)
    monkeypatch.setattr(repo, "replace_cast_screentime_evidence", replace_evidence)
    monkeypatch.setattr(repo, "upsert_cast_screentime_evidence", upsert_cast_screentime_evidence)
    monkeypatch.setattr(repo, "replace_cast_screentime_excluded_sections", replace_excluded)
    monkeypatch.setattr(repo, "replace_run_person_metrics", replace_metrics)
    monkeypatch.setattr(repo, "list_leaderboard", lambda run_id: store["metrics"].get(run_id, []))
    monkeypatch.setattr(repo, "list_segments", lambda run_id: store["segments"].get(run_id, []))
    monkeypatch.setattr(repo, "list_evidence", lambda run_id: store["evidence"].get(run_id, []))
    monkeypatch.setattr(repo, "list_excluded_sections", lambda run_id: store["excluded"].get(run_id, []))
    monkeypatch.setattr(
        repo,
        "list_runs_for_show",
        lambda show_id, limit=20, video_class=None, media_type=None: [
            run
            for run in (
                {**store["video_assets"].get(record["video_asset_id"], {}), **record}
                for record in store["runs"].values()
            )
            if (not video_class or run.get("video_class") == video_class)
            and (not media_type or run.get("media_type") == media_type)
        ][:limit],
    )
    monkeypatch.setattr(repo, "get_publish_version_for_run", get_publish_version_for_run)
    monkeypatch.setattr(repo, "publish_run", publish_run)
    monkeypatch.setattr(repo, "list_publish_versions", list_publish_versions)
    monkeypatch.setattr(repo, "replace_reference_fingerprints_for_run", replace_reference_fingerprints_for_run)
    monkeypatch.setattr(repo, "upsert_suggestion_decision", upsert_suggestion_decision)
    monkeypatch.setattr(repo, "list_suggestion_decisions_for_context", list_suggestion_decisions_for_context)
    monkeypatch.setattr(repo, "upsert_unknown_review_state", upsert_unknown_review_state)
    monkeypatch.setattr(repo, "list_unknown_review_state_for_context", list_unknown_review_state_for_context)
    monkeypatch.setattr(repo, "list_current_published_versions_for_show", list_current_published_versions_for_show)
    monkeypatch.setattr(repo, "list_current_published_versions_for_season", list_current_published_versions_for_season)
    monkeypatch.setattr(repo, "reconcile_stale_runs", reconcile_stale_runs)
    yield


@pytest.fixture(autouse=True)
def fake_storage(monkeypatch):
    monkeypatch.setattr(router_module, "get_s3_bucket", lambda: "test-bucket")
    monkeypatch.setattr(router_module, "get_cdn_base_url", lambda: "https://cdn.example.com")
    monkeypatch.setattr(router_module, "_presigned_put_url", lambda *args, **kwargs: "https://example.com/put")
    monkeypatch.setattr(router_module, "_presigned_get_url", lambda *args, **kwargs: "https://example.com/get")
    monkeypatch.setattr(
        router_module,
        "_head_object",
        lambda *_args, **_kwargs: {"ContentLength": 1024, "ContentType": "video/mp4", "ETag": '"etag-1"'},
    )
    monkeypatch.setattr(
        router_module,
        "_ffprobe_video",
        lambda *_args, **_kwargs: {
            "ok": True,
            "error": None,
            "duration_seconds": 42.5,
            "fps": 23.976,
            "width": 1920,
            "height": 1080,
        },
    )
    monkeypatch.setattr(router_module, "_copy_object", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(router_module, "_delete_object", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        router_module,
        "_mirror_remote_video_to_temp_object",
        lambda **_kwargs: {"bucket": "test-bucket", "content_type": "video/mp4", "etag": "etag-1"},
    )
    monkeypatch.setattr(
        router_module,
        "_copy_existing_object_to_temp_object",
        lambda **_kwargs: {"bucket": "test-bucket", "content_type": "video/mp4", "etag": "etag-1"},
    )
    monkeypatch.setattr(
        router_module, "_read_object_bytes", lambda *_args, **_kwargs: b'{"shots":[{"shot_key":"shot-1"}]}'
    )
    monkeypatch.setattr(
        router_module,
        "_youtube_fetch_video_metadata",
        lambda _video_id: {
            "channel_id": "official-channel-id",
            "channel": "Bravo",
            "uploader": "Bravo",
            "uploader_url": "https://www.youtube.com/@bravo",
            "title": "Official Trailer",
            "webpage_url": "https://www.youtube.com/watch?v=abc123def45",
        },
    )
    monkeypatch.setattr(
        router_module,
        "resolve_youtube_media",
        lambda _video_id: type(
            "Resolution",
            (),
            {"media_urls": ["https://videos.example.com/official-trailer.mp4"], "source": "yt_dlp_manifest"},
        )(),
    )
    monkeypatch.setattr(
        router_module,
        "YouTubeScraper",
        type(
            "DummyYouTubeScraper",
            (),
            {
                "resolve_channel_identity": lambda self, handle, delay=0.25: {
                    "canonical_handle": handle,
                    "channel_id": "official-channel-id",
                }
            },
        ),
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch,
        "start_run",
        lambda run_id: {"run_id": run_id, "accepted": True},
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch,
        "generate_segment_clip",
        lambda run_id, **kwargs: {
            "run_id": run_id,
            "evidence": {
                "segment_key": kwargs["segment_key"],
                "evidence_key": f"clip-{kwargs['mode']}-{kwargs['segment_key']}",
                "evidence_type": "exact_segment_clip" if kwargs["mode"] == "exact" else "timestamp_clip",
                "timestamp_ms": 2500,
                "object_key": f"review/evidence/runs/{run_id}/clips/clip-{kwargs['mode']}-{kwargs['segment_key']}.mp4",
                "content_type": "video/mp4",
                "ttl_expires_at": "2026-03-22T00:00:00+00:00",
                "metadata": {"mode": kwargs["mode"]},
            },
        },
    )
    yield


def _service_headers():
    return {"Authorization": "Bearer test-token"}


def test_upload_complete_and_run_flow():
    client = TestClient(app)
    episode_id = uuid4()

    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    assert create_response.status_code == 200
    upload_session_id = create_response.json()["upload_session_id"]

    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    assert complete_response.status_code == 200
    video_asset = complete_response.json()["video_asset"]
    video_asset_id = video_asset["id"]
    assert video_asset["duration_seconds"] == 42.5
    assert video_asset["source_json"]["probe"]["width"] == 1920

    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={"run_config_json": {"processing_mode": "balanced"}},
    )
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["dispatch_state"] == "queued"
    assert payload["run"]["run_type"] == "cast_screentime"
    assert payload["run"]["status"] == "queued"
    assert payload["run"]["dispatch_status"] == "queued"
    assert payload["run"]["cast_coverage_summary_json"]["candidate_count"] == 1
    assert payload["run"]["candidate_scope_policy_json"]["preferred_facebank_coverage"] is True


def test_create_run_accepts_backend_dispatch_payload(monkeypatch):
    monkeypatch.setattr(
        retained_cast_screentime_dispatch,
        "start_run",
        lambda run_id: {"run_id": run_id, "state": "queued", "job_id": f"backend:{run_id}", "mode": "backend"},
    )

    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]

    response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={"run_config_json": {"processing_mode": "balanced"}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["dispatch_state"] == "queued"
    assert payload["dispatch_result"]["mode"] == "backend"
    assert payload["run"]["dispatch_job_id"].startswith("backend:")
    assert payload["run"]["status"] == "queued"


def test_create_run_marks_dispatch_failures_failed(monkeypatch):
    def _raise_dispatch_error(_run_id):
        raise retained_cast_screentime_dispatch.RetainedCastScreentimeDispatchError("backend runtime unavailable")

    monkeypatch.setattr(retained_cast_screentime_dispatch, "start_run", _raise_dispatch_error)

    client = TestClient(app)
    episode_id = uuid4()

    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]

    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]

    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={"run_config_json": {"processing_mode": "balanced"}},
    )
    assert run_response.status_code == 200
    payload = run_response.json()

    assert payload["dispatch_state"] == "dispatch_failed"
    assert payload["run"]["status"] == "failed"
    assert payload["run"]["error_message"] == "backend runtime unavailable"
    assert payload["run"]["completed_at"] is not None
    assert payload["run"]["review_status"] == "draft"


def test_promo_upload_session_preserves_classification():
    client = TestClient(app)
    season_id = uuid4()

    response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "season",
            "owner_id": str(season_id),
            "filename": "trailer.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
            "video_class": "promo",
            "promo_subtype": "trailer",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["owner_scope"] == "season"
    assert body["media_type"] == "trailer"
    assert body["media_kind"] is None
    assert body["video_class"] == "promo"
    assert body["promo_subtype"] == "trailer"


def test_episode_media_type_requires_episode_owner_scope():
    client = TestClient(app)
    season_id = uuid4()

    response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "season",
            "owner_id": str(season_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
            "media_type": "episode",
        },
    )

    assert response.status_code == 400
    assert "owner_scope=episode" in response.json()["detail"].lower()


def test_import_youtube_trailer_asset():
    client = TestClient(app)
    season_id = uuid4()

    response = client.post(
        "/api/v1/admin/cast-screentime/video-assets/import",
        json={
            "source_mode": "youtube_url",
            "source_url": "https://www.youtube.com/watch?v=abc123def45",
            "owner_scope": "season",
            "owner_id": str(season_id),
            "video_class": "promo",
            "promo_subtype": "trailer",
        },
    )
    assert response.status_code == 200
    video_asset = response.json()["video_asset"]
    assert video_asset["media_type"] == "trailer"
    assert video_asset["media_kind"] is None
    assert video_asset["video_class"] == "promo"
    assert video_asset["promo_subtype"] == "trailer"
    assert video_asset["source_import_type"] == "youtube_url_import"
    assert video_asset["is_publishable"] is False


def test_import_youtube_trailer_rejects_non_official_channel(monkeypatch):
    monkeypatch.setattr(
        router_module,
        "_youtube_fetch_video_metadata",
        lambda _video_id: {
            "channel_id": "different-channel",
            "channel": "Someone Else",
            "uploader": "Someone Else",
            "uploader_url": "https://www.youtube.com/@someoneelse",
        },
    )

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/cast-screentime/video-assets/import",
        json={
            "source_mode": "youtube_url",
            "source_url": "https://www.youtube.com/watch?v=abc123def45",
            "owner_scope": "show",
            "owner_id": str(uuid4()),
            "video_class": "promo",
            "promo_subtype": "trailer",
        },
    )
    assert response.status_code == 403
    assert "official" in response.json()["detail"].lower()


def test_import_social_youtube_row_uses_social_import_type():
    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/cast-screentime/video-assets/import",
        json={
            "source_mode": "social_youtube_row",
            "social_youtube_video_id": str(uuid4()),
            "owner_scope": "show",
            "owner_id": str(uuid4()),
            "video_class": "promo",
            "promo_subtype": "episode_teaser",
        },
    )
    assert response.status_code == 200
    video_asset = response.json()["video_asset"]
    assert video_asset["media_type"] == "extras"
    assert video_asset["media_kind"] == "episode_teaser"
    assert video_asset["source_import_type"] == "social_youtube_import"
    assert video_asset["promo_subtype"] == "episode_teaser"


def test_import_external_url_uses_external_import_type_without_youtube_channel_check(monkeypatch):
    youtube_metadata_calls: list[str] = []

    monkeypatch.setattr(
        router_module,
        "_youtube_fetch_video_metadata",
        lambda video_id: youtube_metadata_calls.append(video_id),
    )

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/cast-screentime/video-assets/import",
        json={
            "source_mode": "external_url",
            "source_url": "https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev/social/youtube/test/trailer.mp4",
            "owner_scope": "season",
            "owner_id": str(uuid4()),
            "video_class": "promo",
            "promo_subtype": "trailer",
        },
    )
    assert response.status_code == 200
    video_asset = response.json()["video_asset"]
    assert video_asset["source_import_type"] == "external_url_import"
    assert youtube_metadata_calls == []


def test_get_video_asset_resolves_legacy_screenalytics_id(monkeypatch):
    legacy_id = str(uuid4())
    canonical_id = str(uuid4())

    monkeypatch.setattr(
        repo,
        "resolve_video_asset",
        lambda video_asset_id: {
            "id": canonical_id,
            "legacy_screenalytics_video_asset_id": legacy_id,
            "show_id": str(uuid4()),
            "season_id": str(uuid4()),
            "episode_id": None,
            "source_url": "s3://test-bucket/source/videos/demo/original.mp4",
            "source_json": {"object_key": "source/videos/demo/original.mp4"},
            "metadata": {"legacy_bridge": {"source_table": "screenalytics.video_assets"}},
            "duration_seconds": 42.5,
            "video_class": "promo",
            "promo_subtype": "trailer",
            "media_type": "trailer",
            "media_kind": None,
            "source_import_type": "external_url_import",
        }
        if video_asset_id == legacy_id
        else None,
    )

    client = TestClient(app)
    response = client.get(f"/api/v1/admin/cast-screentime/video-assets/{legacy_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == canonical_id
    assert payload["legacy_screenalytics_video_asset_id"] == legacy_id
    assert payload["media_type"] == "trailer"


def test_create_run_accepts_legacy_screenalytics_asset_id(monkeypatch):
    legacy_id = str(uuid4())
    canonical_id = str(uuid4())
    show_id = str(uuid4())
    season_id = str(uuid4())

    monkeypatch.setattr(
        repo,
        "resolve_video_asset",
        lambda video_asset_id: {
            "id": canonical_id,
            "legacy_screenalytics_video_asset_id": legacy_id,
            "show_id": show_id,
            "season_id": season_id,
            "episode_id": None,
            "source_url": "s3://test-bucket/source/videos/demo/original.mp4",
            "source_json": {"object_key": "source/videos/demo/original.mp4"},
            "metadata": {"legacy_bridge": {"source_table": "screenalytics.video_assets"}},
            "duration_seconds": 42.5,
            "video_class": "promo",
            "promo_subtype": "trailer",
            "media_type": "trailer",
            "media_kind": None,
            "source_import_type": "external_url_import",
        }
        if video_asset_id == legacy_id
        else None,
    )
    monkeypatch.setattr(repo, "get_video_asset_upload_session_status", lambda _video_asset_id: None)

    client = TestClient(app)
    response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{legacy_id}/runs",
        json={"run_config_json": {"processing_mode": "balanced"}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["video_asset_id"] == canonical_id
    assert payload["dispatch_state"] == "queued"


def test_artifact_registry_covers_phase1_retained_contract():
    assert cast_screentime_artifacts.SHOTS.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.SEGMENTS.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.SCENES.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.EXCLUDED_SECTIONS.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.PERSON_METRICS.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.REFERENCE_FINGERPRINTS.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.CAST_SUGGESTIONS.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.UNKNOWN_REVIEW_QUEUES.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.TITLE_CARD_CANDIDATES.key in cast_screentime_artifacts.ARTIFACT_REGISTRY
    assert cast_screentime_artifacts.CONFESSIONAL_CANDIDATES.key in cast_screentime_artifacts.ARTIFACT_REGISTRY


def test_upload_complete_fails_when_ffprobe_rejects_upload(monkeypatch):
    monkeypatch.setattr(
        router_module, "_ffprobe_video", lambda *_args, **_kwargs: {"ok": False, "error": "ffprobe_failed"}
    )

    client = TestClient(app)
    episode_id = uuid4()

    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]

    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    assert complete_response.status_code == 503
    assert complete_response.json()["detail"] == "Video probe failed: ffprobe_failed"


def test_internal_finalize_and_reads():
    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]
    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={},
    )
    run_id = run_response.json()["run"]["id"]

    response = client.patch(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/status",
        headers=_service_headers(),
        json={"status": "running", "manifest_key": "derived/runs/test/manifest.json"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "running"

    response = client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/finalize",
        headers=_service_headers(),
        json={"status": "success", "manifest_key": "derived/runs/test/manifest.json", "effective_runtime_seconds": 60},
    )
    assert response.status_code == 200
    assert response.json()["review_status"] == "ready_for_review"

    response = client.get(f"/api/v1/admin/cast-screentime/runs/{run_id}")
    assert response.status_code == 200
    assert response.json()["run_type"] == "cast_screentime"

    artifact_upsert = client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/artifacts:upsert",
        headers=_service_headers(),
        json={
            "artifacts": [
                {
                    "artifact_key": "shots.json",
                    "artifact_kind": "shots",
                    "s3_key": f"derived/runs/{run_id}/shots.json",
                    "content_type": "application/json",
                }
            ]
        },
    )
    assert artifact_upsert.status_code == 200

    artifact_response = client.get(f"/api/v1/admin/cast-screentime/runs/{run_id}/artifacts/shots.json")
    assert artifact_response.status_code == 200
    assert artifact_response.json()["payload"]["shots"][0]["shot_key"] == "shot-1"


def test_review_status_rejects_non_success_run():
    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]
    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={},
    )
    run_id = run_response.json()["run"]["id"]

    response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/review-status",
        json={"review_status": "ready_for_review", "notes": {}},
    )

    assert response.status_code == 409
    assert "successful runs can enter review flow" in response.json()["detail"].lower()


def test_generate_segment_clip_persists_clip_evidence():
    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]
    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={},
    )
    run_id = run_response.json()["run"]["id"]

    response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/segments/segment-1/clip",
        json={"mode": "timestamp", "duration_seconds": 10, "ttl_days": 7},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["evidence"]["content_type"] == "video/mp4"
    assert body["evidence"]["evidence_type"] == "timestamp_clip"
    assert body["evidence"]["public_url"] is None or body["evidence"]["public_url"].endswith(".mp4")


def test_reconcile_stale_runs_marks_running_runs_failed():
    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset = complete_response.json()["video_asset"]
    video_asset_id = video_asset["id"]
    show_id = video_asset["show_id"]
    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={},
    )
    run_id = run_response.json()["run"]["id"]

    client.patch(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/status",
        headers=_service_headers(),
        json={"status": "running"},
    )

    response = client.post(
        f"/api/v1/admin/cast-screentime/runs/reconcile-stale?show_id={show_id}&stale_after_seconds=60",
    )
    assert response.status_code == 200
    body = response.json()
    assert body["reconciled_run_count"] == 1
    assert body["runs"][0]["status"] == "failed"
    assert body["runs"][0]["error_message"] == "worker_heartbeat_expired"


def test_publish_episode_run_creates_current_version_and_rollups(monkeypatch):
    def _read_object_bytes(_bucket, key):
        if key.endswith("reference_fingerprints.json"):
            return (
                b'[{"scene_key":"scene-1","fingerprint_type":"scene_signature",'
                b'"fingerprint_hash":"hash-1","start_ms":0,"end_ms":1000,'
                b'"duration_ms":1000,"metadata":{"composition_type":"single_subject"}}]'
            )
        return b'{"shots":[{"shot_key":"shot-1"}]}'

    monkeypatch.setattr(router_module, "_read_object_bytes", _read_object_bytes)

    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset = complete_response.json()["video_asset"]
    video_asset_id = video_asset["id"]
    show_id = video_asset["show_id"]
    season_id = video_asset["season_id"]

    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={},
    )
    run_id = run_response.json()["run"]["id"]

    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/artifacts:upsert",
        headers=_service_headers(),
        json={
            "artifacts": [
                {
                    "artifact_key": "reference_fingerprints.json",
                    "artifact_kind": "reference_fingerprints",
                    "s3_key": f"derived/runs/{run_id}/reference_fingerprints.json",
                    "content_type": "application/json",
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/segments:replace",
        headers=_service_headers(),
        json={
            "segments": [
                {
                    "segment_key": "segment-1",
                    "person_id": str(uuid4()),
                    "start_ms": 0,
                    "end_ms": 10000,
                    "duration_ms": 10000,
                    "frame_count": 120,
                    "confidence_score": 0.93,
                    "similarity_score": 0.9,
                    "pose_bucket": "frontal",
                    "assignment_source": "retained_backend_runtime",
                    "is_counted": True,
                    "classification_json": {},
                    "metadata": {"display_name": "Test Person"},
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/excluded-sections:replace",
        headers=_service_headers(),
        json={
            "excluded_sections": [
                {
                    "section_key": "cold-open",
                    "section_type": "intro",
                    "start_ms": 2500,
                    "end_ms": 5000,
                    "duration_ms": 2500,
                    "detection_source": "manual",
                    "confidence_score": 1.0,
                    "metadata": {"reason": "credits"},
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/person-metrics:replace",
        headers=_service_headers(),
        json={
            "metrics": [
                {
                    "person_id": str(uuid4()),
                    "screen_time_seconds": 12.5,
                    "frame_count": 120,
                    "confidence_avg": 0.93,
                    "metadata": {"display_name": "Test Person"},
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/finalize",
        headers=_service_headers(),
        json={"status": "success", "effective_runtime_seconds": 42.5},
    )
    review_response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/review-status",
        json={"review_status": "in_review", "notes": {}},
    )
    assert review_response.status_code == 200
    approve_response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/review-status",
        json={"review_status": "approved", "notes": {}},
    )
    assert approve_response.status_code == 200

    publish_response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/publish",
        json={"notes": {"source": "pytest"}},
    )
    assert publish_response.status_code == 200
    publish_body = publish_response.json()
    assert publish_body["publish_version"]["version_number"] == 1
    assert publish_body["publish_version"]["is_current"] is True
    assert publish_body["reference_fingerprint_count"] == 1
    assert publish_body["publication_mode"] == "canonical_episode"

    history_response = client.get(f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/publish-history")
    assert history_response.status_code == 200
    assert history_response.json()["publish_history"][0]["version_number"] == 1
    assert history_response.json()["publish_history"][0]["publication_mode"] == "canonical_episode"

    show_rollup = client.get(f"/api/v1/admin/cast-screentime/shows/{show_id}/published-rollups")
    assert show_rollup.status_code == 200
    assert show_rollup.json()["published_asset_count"] == 1
    assert show_rollup.json()["leaderboard"][0]["screen_time_seconds"] == 7.5

    season_rollup = client.get(f"/api/v1/admin/cast-screentime/seasons/{season_id}/published-rollups")
    assert season_rollup.status_code == 200
    assert season_rollup.json()["published_asset_count"] == 1


def test_suggestion_and_unknown_review_decisions_persist_for_run_context(monkeypatch):
    def _read_object_bytes(_bucket, key):
        if key.endswith("cast_suggestions.json"):
            return (
                b'[{"suggestion_key":"suggest-person-1","person_id":"11111111-1111-1111-1111-111111111111",'
                b'"display_name":"Test Person","support_count":2,"scene_count":1,"total_duration_ms":1000}]'
            )
        if key.endswith("unknown_review_queues.json"):
            return (
                b'[{"queue_key":"unknown-queue-demo","queue_group":"11111111-1111-1111-1111-111111111111",'
                b'"candidate_person_id":"11111111-1111-1111-1111-111111111111","candidate_display_name":"Test Person",'
                b'"support_count":2,"scene_count":1,"total_duration_ms":1000,"escalation_level":"season",'
                b'"recommended_action":"season_review"}]'
            )
        return b"{}"

    monkeypatch.setattr(router_module, "_read_object_bytes", _read_object_bytes)

    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]
    run_response = client.post(f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs", json={})
    run_id = run_response.json()["run"]["id"]

    artifact_upsert = client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/artifacts:upsert",
        headers=_service_headers(),
        json={
            "artifacts": [
                {
                    "artifact_key": "cast_suggestions.json",
                    "artifact_kind": "cast_suggestions",
                    "s3_key": f"derived/runs/{run_id}/cast_suggestions.json",
                    "content_type": "application/json",
                },
                {
                    "artifact_key": "unknown_review_queues.json",
                    "artifact_kind": "unknown_review_queues",
                    "s3_key": f"derived/runs/{run_id}/unknown_review_queues.json",
                    "content_type": "application/json",
                },
            ]
        },
    )
    assert artifact_upsert.status_code == 200

    decision_response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/suggestions/suggest-person-1/decision",
        json={"decision": "accept", "decision_scope": "season", "notes": {"source": "pytest"}},
    )
    assert decision_response.status_code == 200
    assert decision_response.json()["decision"]["decision"] == "accept"

    unknown_response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/unknown-review/unknown-queue-demo/decision",
        json={"decision": "defer", "decision_scope": "show", "notes": {"source": "pytest"}},
    )
    assert unknown_response.status_code == 200
    assert unknown_response.json()["decision"]["decision"] == "defer"

    state_response = client.get(f"/api/v1/admin/cast-screentime/runs/{run_id}/decision-state")
    assert state_response.status_code == 200
    payload = state_response.json()
    assert payload["suggestion_decisions"][0]["suggestion_key"] == "suggest-person-1"
    assert payload["unknown_review_state"][0]["queue_key"] == "unknown-queue-demo"
    assert payload["rerun_required_for_metrics"] is True
    assert "rerun" in payload["decision_effect_summary"].lower()


def test_review_summary_returns_reviewed_leaderboard_and_publication_mode():
    client = TestClient(app)
    episode_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "episode",
            "owner_id": str(episode_id),
            "filename": "episode.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]
    person_id = uuid4()
    run_response = client.post(f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs", json={})
    run_id = run_response.json()["run"]["id"]

    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/segments:replace",
        headers=_service_headers(),
        json={
            "segments": [
                {
                    "segment_key": "segment-1",
                    "person_id": str(person_id),
                    "start_ms": 0,
                    "end_ms": 8000,
                    "duration_ms": 8000,
                    "frame_count": 96,
                    "confidence_score": 0.91,
                    "similarity_score": 0.88,
                    "pose_bucket": "frontal",
                    "assignment_source": "retained_backend_runtime",
                    "is_counted": True,
                    "classification_json": {},
                    "metadata": {"display_name": "Test Person"},
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/excluded-sections:replace",
        headers=_service_headers(),
        json={
            "excluded_sections": [
                {
                    "section_key": "cold-open",
                    "section_type": "intro",
                    "start_ms": 1000,
                    "end_ms": 3000,
                    "duration_ms": 2000,
                    "detection_source": "manual",
                    "confidence_score": 1.0,
                    "metadata": {"reason": "credits"},
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/person-metrics:replace",
        headers=_service_headers(),
        json={
            "metrics": [
                {
                    "person_id": str(person_id),
                    "screen_time_seconds": 12.5,
                    "frame_count": 96,
                    "confidence_avg": 0.91,
                    "metadata": {"display_name": "Test Person"},
                }
            ]
        },
    )
    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/finalize",
        headers=_service_headers(),
        json={"status": "success", "effective_runtime_seconds": 42.5},
    )

    response = client.get(f"/api/v1/admin/cast-screentime/runs/{run_id}/review-summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["publication_mode"] == "canonical_episode"
    assert payload["is_canonical_publication"] is True
    assert payload["excluded_section_count"] == 1
    assert payload["excluded_overlap_ms"] == 2000
    assert payload["raw_leaderboard"][0]["screen_time_seconds"] == 12.5
    assert payload["reviewed_leaderboard"][0]["screen_time_seconds"] == 6.0


def test_publish_allows_supplementary_internal_reference_without_rollup_contamination():
    client = TestClient(app)
    season_id = uuid4()
    create_response = client.post(
        "/api/v1/admin/cast-screentime/upload-sessions",
        json={
            "owner_scope": "season",
            "owner_id": str(season_id),
            "filename": "trailer.mp4",
            "content_type": "video/mp4",
            "expected_size_bytes": 1024,
            "video_class": "promo",
            "promo_subtype": "trailer",
        },
    )
    upload_session_id = create_response.json()["upload_session_id"]
    complete_response = client.post(
        f"/api/v1/admin/cast-screentime/upload-sessions/{upload_session_id}/complete",
        json={"upload_session_id": upload_session_id},
    )
    video_asset_id = complete_response.json()["video_asset"]["id"]
    run_response = client.post(
        f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs",
        json={},
    )
    run_id = run_response.json()["run"]["id"]

    client.post(
        f"/api/v1/internal/screenalytics/cast-screentime/runs/{run_id}/finalize",
        headers=_service_headers(),
        json={"status": "success", "effective_runtime_seconds": 42.5},
    )
    client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/review-status",
        json={"review_status": "in_review", "notes": {}},
    )
    client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/review-status",
        json={"review_status": "approved", "notes": {}},
    )

    response = client.post(
        f"/api/v1/admin/cast-screentime/runs/{run_id}/publish",
        json={"notes": {"source": "pytest"}},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["publication_mode"] == "supplementary_reference"
    assert payload["publish_version"]["is_current"] is True

    history_response = client.get(f"/api/v1/admin/cast-screentime/video-assets/{video_asset_id}/publish-history")
    assert history_response.status_code == 200
    assert history_response.json()["publish_history"][0]["publication_mode"] == "supplementary_reference"

    published_show_id = complete_response.json()["video_asset"]["show_id"]
    show_rollup = client.get(f"/api/v1/admin/cast-screentime/shows/{published_show_id}/published-rollups")
    assert show_rollup.status_code == 200
    assert show_rollup.json()["published_asset_count"] == 0


def test_list_show_runs_rejects_service_role_without_internal_secret_header(monkeypatch, no_admin_override):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    client = TestClient(app)
    response = client.get(
        f"/api/v1/admin/cast-screentime/shows/{uuid4()}/runs",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403
    assert "Allowlist admin access required" in response.json()["detail"]


def test_list_show_runs_rejects_service_role_with_invalid_internal_secret_header(monkeypatch, no_admin_override):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    client = TestClient(app)
    response = client.get(
        f"/api/v1/admin/cast-screentime/shows/{uuid4()}/runs",
        headers={
            "Authorization": f"Bearer {token}",
            "X-TRR-Internal-Admin-Secret": "wrong-secret",
        },
    )

    assert response.status_code == 403
    assert "Allowlist admin access required" in response.json()["detail"]


def test_list_show_runs_allows_service_role_with_valid_internal_secret_header(monkeypatch, no_admin_override):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setattr(repo, "list_runs_for_show", lambda show_id, limit=20, video_class=None, media_type=None: [])

    client = TestClient(app)
    response = client.get(
        f"/api/v1/admin/cast-screentime/shows/{uuid4()}/runs",
        headers={
            "Authorization": f"Bearer {token}",
            "X-TRR-Internal-Admin-Secret": "internal-secret",
        },
    )

    assert response.status_code == 200
    assert response.json()["runs"] == []
