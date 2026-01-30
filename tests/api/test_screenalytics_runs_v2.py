from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.repositories import screenalytics_runs as repo


@pytest.fixture(autouse=True)
def set_service_token(monkeypatch):
    monkeypatch.setenv("SCREENALYTICS_SERVICE_TOKEN", "test-token")
    yield


@pytest.fixture(autouse=True)
def fake_repo(monkeypatch):
    store: dict[str, dict] = {
        "video_assets": {},
        "runs": {},
        "artifacts": {},
        "metrics": {},
        "unknown_clusters": {},
    }

    def create_video_asset(payload):
        video_asset_id = str(uuid4())
        record = {**payload, "id": video_asset_id}
        store["video_assets"][video_asset_id] = record
        return record

    def get_video_asset(video_asset_id):
        return store["video_assets"].get(video_asset_id)

    def create_run(payload):
        run_id = str(uuid4())
        record = {
            "id": run_id,
            "video_asset_id": payload["video_asset_id"],
            "status": "pending",
            "run_config_json": payload.get("run_config_json", {}),
            "config_hash": payload.get("config_hash"),
            "candidate_cast_snapshot_json": payload.get("candidate_cast_snapshot_json", []),
            "manifest_key": None,
            "started_at": None,
            "completed_at": None,
            "error_message": None,
        }
        store["runs"][run_id] = record
        return record

    def get_run(run_id):
        return store["runs"].get(run_id)

    def get_run_with_video_asset(run_id):
        run = store["runs"].get(run_id)
        if not run:
            return None
        video_asset = store["video_assets"].get(run["video_asset_id"], {})
        return {**run, **video_asset}

    def update_run(run_id, payload):
        run = store["runs"].get(run_id)
        if not run:
            return None
        run.update(payload)
        return run

    def upsert_run_artifacts(run_id, artifacts):
        run_store = store["artifacts"].setdefault(run_id, {})
        for artifact in artifacts:
            run_store[artifact["artifact_key"]] = {**artifact, "run_id": run_id}
        return list(run_store.values())

    def upsert_run_person_metrics(run_id, metrics):
        run_store = store["metrics"].setdefault(run_id, {})
        for metric in metrics:
            run_store[str(metric["person_id"])] = {**metric, "run_id": run_id}
        return list(run_store.values())

    def list_leaderboard(run_id):
        entries = list(store["metrics"].get(run_id, {}).values())
        return sorted(entries, key=lambda item: float(item.get("screen_time_seconds", 0)), reverse=True)

    def list_unknown_clusters(run_id):
        return list(store["unknown_clusters"].get(run_id, {}).values())

    def upsert_unknown_clusters(run_id, clusters):
        run_store = store["unknown_clusters"].setdefault(run_id, {})
        for cluster in clusters:
            run_store[cluster["cluster_id"]] = {**cluster, "run_id": run_id}
        return list(run_store.values())

    def assign_unknown_cluster(run_id, cluster_id, person_id, assigned_by):
        run_store = store["unknown_clusters"].setdefault(run_id, {})
        cluster = run_store.get(cluster_id)
        if not cluster:
            return None
        cluster.update(
            {
                "assigned_person_id": person_id,
                "assigned_by": assigned_by,
                "assigned_at": "2025-01-01T00:00:00Z",
            }
        )
        return cluster

    monkeypatch.setattr(repo, "create_video_asset", create_video_asset)
    monkeypatch.setattr(repo, "get_video_asset", get_video_asset)
    monkeypatch.setattr(repo, "create_run", create_run)
    monkeypatch.setattr(repo, "get_run", get_run)
    monkeypatch.setattr(repo, "get_run_with_video_asset", get_run_with_video_asset)
    monkeypatch.setattr(repo, "update_run", update_run)
    monkeypatch.setattr(repo, "upsert_run_artifacts", upsert_run_artifacts)
    monkeypatch.setattr(repo, "upsert_run_person_metrics", upsert_run_person_metrics)
    monkeypatch.setattr(repo, "list_leaderboard", list_leaderboard)
    monkeypatch.setattr(repo, "list_unknown_clusters", list_unknown_clusters)
    monkeypatch.setattr(repo, "upsert_unknown_clusters", upsert_unknown_clusters)
    monkeypatch.setattr(repo, "assign_unknown_cluster", assign_unknown_cluster)
    yield


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_requires_token():
    client = TestClient(app)
    response = client.post("/api/v1/screenalytics/v2/video-assets", json={})
    assert response.status_code == 401


def test_screenalytics_flow():
    show_id = uuid4()
    season_id = uuid4()
    episode_id = uuid4()
    person_id = uuid4()

    client = TestClient(app)
    response = client.post(
        "/api/v1/screenalytics/v2/video-assets",
        headers=_auth_headers(),
        json={
            "show_id": str(show_id),
            "season_id": str(season_id),
            "episode_id": str(episode_id),
            "source_url": "https://example.com/video.mp4",
            "duration_seconds": 123,
        },
    )
    assert response.status_code == 200
    video_asset_id = response.json()["id"]

    response = client.post(
        "/api/v1/screenalytics/v2/runs",
        headers=_auth_headers(),
        json={
            "video_asset_id": video_asset_id,
            "run_config_json": {"foo": "bar"},
        },
    )
    assert response.status_code == 200
    run_id = response.json()["id"]

    response = client.patch(
        f"/api/v1/screenalytics/v2/runs/{run_id}/status",
        headers=_auth_headers(),
        json={"status": "running"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "running"

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/artifacts:upsert",
        headers=_auth_headers(),
        json={
            "artifacts": [
                {
                    "artifact_key": "tracks.parquet",
                    "artifact_kind": "tracks",
                    "s3_key": "s3://bucket/tracks.parquet",
                }
            ]
        },
    )
    assert response.status_code == 200
    assert len(response.json()) == 1

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/person-metrics:upsert",
        headers=_auth_headers(),
        json={
            "metrics": [
                {
                    "person_id": str(person_id),
                    "screen_time_seconds": 42,
                    "frame_count": 100,
                    "confidence_avg": 0.9,
                }
            ]
        },
    )
    assert response.status_code == 200
    assert len(response.json()) == 1

    response = client.get(
        f"/api/v1/screenalytics/v2/runs/{run_id}/leaderboard",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert float(response.json()[0]["screen_time_seconds"]) == 42.0


def test_unknown_clusters_flow():
    client = TestClient(app)
    # create minimal run + video asset
    show_id = uuid4()
    season_id = uuid4()
    episode_id = uuid4()
    person_id = uuid4()

    response = client.post(
        "/api/v1/screenalytics/v2/video-assets",
        headers=_auth_headers(),
        json={
            "show_id": str(show_id),
            "season_id": str(season_id),
            "episode_id": str(episode_id),
            "source_url": "https://example.com/video-2.mp4",
        },
    )
    video_asset_id = response.json()["id"]

    response = client.post(
        "/api/v1/screenalytics/v2/runs",
        headers=_auth_headers(),
        json={
            "video_asset_id": video_asset_id,
            "run_config_json": {"foo": "bar"},
        },
    )
    run_id = response.json()["id"]

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/unknown-clusters:upsert",
        headers=_auth_headers(),
        json={
            "clusters": [
                {
                    "cluster_id": "c1",
                    "track_count": 2,
                    "preview_s3_key": "s3://bucket/preview.jpg",
                }
            ]
        },
    )
    assert response.status_code == 200

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/unknown-clusters/c1/assign",
        headers=_auth_headers(),
        json={
            "person_id": str(person_id),
            "assigned_by": "tester",
        },
    )
    assert response.status_code == 200
    assert response.json()["assigned_person_id"] == str(person_id)

    response = client.get(
        f"/api/v1/screenalytics/v2/runs/{run_id}/unknown-clusters",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert len(response.json()) == 1


def test_idempotent_upserts():
    client = TestClient(app)
    show_id = uuid4()
    season_id = uuid4()
    episode_id = uuid4()
    person_id = uuid4()

    response = client.post(
        "/api/v1/screenalytics/v2/video-assets",
        headers=_auth_headers(),
        json={
            "show_id": str(show_id),
            "season_id": str(season_id),
            "episode_id": str(episode_id),
            "source_url": "https://example.com/video-3.mp4",
        },
    )
    video_asset_id = response.json()["id"]

    response = client.post(
        "/api/v1/screenalytics/v2/runs",
        headers=_auth_headers(),
        json={
            "video_asset_id": video_asset_id,
            "run_config_json": {"foo": "bar"},
        },
    )
    run_id = response.json()["id"]

    payload = {
        "artifacts": [
            {
                "artifact_key": "tracks.parquet",
                "artifact_kind": "tracks",
                "s3_key": "s3://bucket/tracks.parquet",
            }
        ]
    }
    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/artifacts:upsert",
        headers=_auth_headers(),
        json=payload,
    )
    assert response.status_code == 200
    first_len = len(response.json())

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/artifacts:upsert",
        headers=_auth_headers(),
        json=payload,
    )
    assert response.status_code == 200
    assert len(response.json()) == first_len

    metrics_payload = {
        "metrics": [
            {
                "person_id": str(person_id),
                "screen_time_seconds": 42,
                "frame_count": 100,
                "confidence_avg": 0.9,
            }
        ]
    }
    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/person-metrics:upsert",
        headers=_auth_headers(),
        json=metrics_payload,
    )
    assert response.status_code == 200
    first_len = len(response.json())

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/person-metrics:upsert",
        headers=_auth_headers(),
        json=metrics_payload,
    )
    assert response.status_code == 200
    assert len(response.json()) == first_len

    clusters_payload = {
        "clusters": [
            {
                "cluster_id": "c1",
                "track_count": 2,
                "preview_s3_key": "s3://bucket/preview.jpg",
            }
        ]
    }
    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/unknown-clusters:upsert",
        headers=_auth_headers(),
        json=clusters_payload,
    )
    assert response.status_code == 200
    first_len = len(response.json())

    response = client.post(
        f"/api/v1/screenalytics/v2/runs/{run_id}/unknown-clusters:upsert",
        headers=_auth_headers(),
        json=clusters_payload,
    )
    assert response.status_code == 200
    assert len(response.json()) == first_len
