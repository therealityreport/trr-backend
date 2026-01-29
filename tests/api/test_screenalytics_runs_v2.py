from __future__ import annotations

import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

os.environ["TRR_SCREENALYTICS_ONLY"] = "1"

from api.main import app
from trr_backend.db import pg


@pytest.fixture(autouse=True)
def set_service_token(monkeypatch):
    monkeypatch.setenv("SCREENALYTICS_SERVICE_TOKEN", "test-token")
    yield


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


@pytest.fixture
def seeded_core_rows():
    show_id = uuid4()
    season_id = uuid4()
    episode_id = uuid4()
    person_id = uuid4()

    pg.execute_returning(
        "INSERT INTO core.shows (id, name) VALUES (%s, %s) RETURNING id",
        [str(show_id), "Test Show"],
    )
    pg.execute_returning(
        "INSERT INTO core.seasons (id, show_id, season_number) VALUES (%s, %s, %s) RETURNING id",
        [str(season_id), str(show_id), 1],
    )
    pg.execute_returning(
        "INSERT INTO core.episodes (id, show_id, season_id, season_number, episode_number) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        [str(episode_id), str(show_id), str(season_id), 1, 1],
    )
    pg.execute_returning(
        "INSERT INTO core.people (id, full_name) VALUES (%s, %s) RETURNING id",
        [str(person_id), "Test Person"],
    )

    return {
        "show_id": show_id,
        "season_id": season_id,
        "episode_id": episode_id,
        "person_id": person_id,
    }


def test_requires_token():
    client = TestClient(app)
    response = client.post("/api/v1/screenalytics/v2/video-assets", json={})
    assert response.status_code == 401


def test_screenalytics_flow(seeded_core_rows):
    client = TestClient(app)
    response = client.post(
        "/api/v1/screenalytics/v2/video-assets",
        headers=_auth_headers(),
        json={
            "show_id": str(seeded_core_rows["show_id"]),
            "episode_id": str(seeded_core_rows["episode_id"]),
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
                    "person_id": str(seeded_core_rows["person_id"]),
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

    pg.execute_returning(
        "INSERT INTO core.shows (id, name) VALUES (%s, %s) RETURNING id",
        [str(show_id), "Test Show 2"],
    )
    pg.execute_returning(
        "INSERT INTO core.seasons (id, show_id, season_number) VALUES (%s, %s, %s) RETURNING id",
        [str(season_id), str(show_id), 1],
    )
    pg.execute_returning(
        "INSERT INTO core.episodes (id, show_id, season_id, season_number, episode_number) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        [str(episode_id), str(show_id), str(season_id), 1, 1],
    )
    pg.execute_returning(
        "INSERT INTO core.people (id, full_name) VALUES (%s, %s) RETURNING id",
        [str(person_id), "Test Person 2"],
    )

    video_asset = pg.execute_returning(
        "INSERT INTO screenalytics.video_assets (show_id, episode_id, source_url) VALUES (%s, %s, %s) RETURNING id",
        [str(show_id), str(episode_id), "https://example.com/video-2.mp4"],
    )[0]
    run = pg.execute_returning(
        "INSERT INTO screenalytics.runs_v2 (video_asset_id) VALUES (%s) RETURNING id",
        [video_asset["id"]],
    )[0]
    run_id = run["id"]

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


def test_idempotent_upserts(seeded_core_rows):
    client = TestClient(app)
    video_asset = pg.execute_returning(
        "INSERT INTO screenalytics.video_assets (show_id, episode_id, source_url) VALUES (%s, %s, %s) RETURNING id",
        [
            str(seeded_core_rows["show_id"]),
            str(seeded_core_rows["episode_id"]),
            "https://example.com/video-3.mp4",
        ],
    )[0]
    run = pg.execute_returning(
        "INSERT INTO screenalytics.runs_v2 (video_asset_id) VALUES (%s) RETURNING id",
        [video_asset["id"]],
    )[0]
    run_id = run["id"]

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
                "person_id": str(seeded_core_rows["person_id"]),
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
