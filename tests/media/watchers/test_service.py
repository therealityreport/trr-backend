from __future__ import annotations

import io
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from PIL import Image

from trr_backend.media.watchers import service, sources

NOW = datetime(2026, 8, 6, 12, tzinfo=UTC)
WATCH_ID = "11111111-1111-1111-1111-111111111111"
SHOW_ID = "22222222-2222-2222-2222-222222222222"
SEASON_ID = "33333333-3333-3333-3333-333333333333"


def _image_bytes(color: tuple[int, int, int] = (1, 2, 3)) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (2, 3), color).save(output, format="JPEG")
    return output.getvalue()


def _watch(*, baseline_completed_at: object = NOW, source_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "id": WATCH_ID,
        "show_id": SHOW_ID,
        "season_id": SEASON_ID,
        "target_season_number": 7,
        "nbcumv_show_id": "490e731c-d85f-474f-945b-b9681dc1931b",
        "bravo_show_uuid": "44444444-4444-4444-4444-444444444444",
        "r2_prefix": "shows/the-watch/season-7",
        "sources": ["bravo"],
        "resource_types": ["image"],
        "poll_interval_seconds": 60,
        "overlap_seconds": 300,
        "source_season_rules": {},
        "baseline_completed_at": baseline_completed_at,
        "source_state": dict(source_state or {}),
    }


def _candidate(*, source_asset_id: str = "image-1", season: object = 7) -> dict[str, Any]:
    return {
        "source": "bravo",
        "source_asset_id": source_asset_id,
        "resource_type": "media/image",
        "media_type": "image",
        "changed_at": "2026-08-06T12:00:00Z",
        "created_at": "2026-08-06T12:00:00Z",
        "original_url": "https://www.bravotv.com/sites/bravo/files/watch.jpg",
        "download_url": "https://www.bravotv.com/sites/bravo/files/watch.jpg",
        "filename": "watch.jpg",
        "mime_type": "image/jpeg",
        "caption": "Season 7 portrait",
        "raw_season_fields": {"season_number": season},
        "people": [],
        "provenance": {"adapter": "fixture", "source_id": source_asset_id},
        "raw_record": {"id": source_asset_id},
    }


def _result(*candidates: Mapping[str, Any], complete: bool = True, continuation: str | None = None):
    return sources.SourceDiscoveryResult(
        candidates=tuple(dict(candidate) for candidate in candidates),
        complete=complete,
        continuation=continuation,
        pages_fetched=1,
        terminal_streams=("fixture",) if complete else (),
        provenance={"adapter": "fixture", **({"incomplete_reason": "page_cap"} if not complete else {})},
    )


class _Repository:
    def __init__(self) -> None:
        self.observations: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.finished: list[dict[str, Any]] = []
        self.journals: list[dict[str, Any]] = []
        self.run_count = 0

    def start_baseline_generation(self, **_kwargs: Any) -> dict[str, Any]:
        return {"id": "baseline-1"}

    def start_run(self, **_kwargs: Any) -> dict[str, Any]:
        self.run_count += 1
        return {"id": f"run-{self.run_count}"}

    def heartbeat_lease(self, **_kwargs: Any) -> bool:
        return True

    def upsert_observation(self, **kwargs: Any) -> dict[str, Any]:
        key = (kwargs["watch_id"], kwargs["source"], kwargs["source_asset_id"])
        self.observations[key] = {
            "source_fingerprint": dict(kwargs["source_fingerprint"]),
            "acquisition_state": kwargs["acquisition_state"],
            "revalidate_after": kwargs["revalidate_after"],
            "metadata": dict(kwargs["metadata"]),
        }
        return {"id": "observation"}

    def update_run_journal(self, **kwargs: Any) -> dict[str, Any]:
        self.journals.append(kwargs)
        return {"id": kwargs["run_id"]}

    def finish_run(self, **kwargs: Any) -> dict[str, Any]:
        self.finished.append(kwargs)
        return {"id": kwargs["run_id"]}


class _R2:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.puts = 0

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]:
        key = kwargs["Key"]
        if key not in self.objects:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
            raise error
        return self.objects[key]

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]:
        self.puts += 1
        self.objects[kwargs["Key"]] = {
            "ContentLength": len(kwargs["Body"]),
            "ContentType": kwargs["ContentType"],
            "Metadata": dict(kwargs["Metadata"]),
            "ETag": '"etag"',
        }
        return {"ETag": '"etag"'}


def _service(
    repo: _Repository,
    r2: _R2,
    result: sources.SourceDiscoveryResult,
    *,
    data: bytes = _image_bytes(),
    commit: Callable[..., str | None] | None = None,
) -> tuple[service.WatcherAcquisitionService, list[str]]:
    downloads: list[str] = []

    def downloader(candidate: Mapping[str, Any]) -> service.DownloadedMedia:
        downloads.append(str(candidate["source_asset_id"]))
        return service.DownloadedMedia(
            data=data,
            content_type="image/jpeg",
            width=2,
            height=3,
            source_url=str(candidate["download_url"]),
        )

    def lookup(watch_id: str, source: str, source_asset_id: str) -> Mapping[str, Any] | None:
        return repo.observations.get((watch_id, source, source_asset_id))

    def default_commit(**kwargs: Any) -> str:
        candidate = kwargs["candidate"]
        key = (kwargs["watch"]["id"], candidate["source"], candidate["source_asset_id"])
        repo.observations[key]["acquisition_state"] = "db_committed"
        repo.observations[key]["revalidate_after"] = NOW + timedelta(hours=6)
        repo.observations[key]["metadata"]["revision"] = kwargs["stored"].sha256
        return "asset-1"

    watcher = service.WatcherAcquisitionService(
        repository=repo,
        discover_bravo=lambda *_args, **_kwargs: result,
        discover_nbcumv=lambda *_args, **_kwargs: result,
        downloader=downloader,
        r2_client=r2,
        r2_bucket="fixture-bucket",
        r2_url_builder=lambda key: f"https://cdn.example/{key}",
        observation_lookup=lookup,
        commit_acquisition=commit or default_commit,
        now=lambda: NOW,
    )
    watcher._latest_bravo_continuation = lambda _watch_id: None  # type: ignore[method-assign]
    return watcher, downloads


def test_baseline_observes_without_bytes_then_explicit_backfill_acquires_unchanged_source(monkeypatch) -> None:
    repo, r2, candidate = _Repository(), _R2(), _candidate()
    watcher, downloads = _service(repo, r2, _result(candidate))
    def finish_baseline(**kwargs: Any) -> service.WatchRunResult:
        return service.WatchRunResult(
            run_id=kwargs["run_id"],
            status="completed",
            summary=dict(kwargs["summary"]),
            source_state_after=dict(kwargs["source_state_after"]),
            continuation={},
        )

    monkeypatch.setattr(watcher, "_finish_baseline_atomic", finish_baseline)

    baseline = watcher.run(_watch(baseline_completed_at=None), lease_owner="worker", lease_fence=1)
    assert baseline.status == "completed"
    assert baseline.summary["observed"] == 1
    assert downloads == []
    assert repo.observations[(WATCH_ID, "bravo", "image-1")]["acquisition_state"] == "observed_without_bytes"

    backfill = watcher.run(_watch(), lease_owner="worker", lease_fence=2, backfill=True)
    assert backfill.status == "completed"
    assert backfill.summary["added"] == 1
    assert downloads == ["image-1"]
    assert r2.puts == 1


def test_byte_backed_unchanged_candidate_skips_before_ttl_then_revises_after_ttl() -> None:
    repo, r2, candidate = _Repository(), _R2(), _candidate()
    first, first_downloads = _service(repo, r2, _result(candidate), data=_image_bytes((1, 2, 3)))
    assert first.run(_watch(), lease_owner="worker", lease_fence=1).summary["added"] == 1

    second, second_downloads = _service(repo, r2, _result(candidate), data=_image_bytes((4, 5, 6)))
    unchanged = second.run(_watch(), lease_owner="worker", lease_fence=2)
    assert unchanged.summary["unchanged"] == 1
    assert second_downloads == []

    repo.observations[(WATCH_ID, "bravo", "image-1")]["revalidate_after"] = NOW - timedelta(seconds=1)
    revised = second.run(_watch(), lease_owner="worker", lease_fence=3)
    assert revised.summary["revised"] == 1
    assert second_downloads == ["image-1"]
    assert r2.puts == 2
    assert first_downloads == ["image-1"]


def test_unresolved_is_show_link_eligible_but_never_season_linked() -> None:
    unresolved = service.qualify_candidate(_candidate(season=None), _watch())
    confirmed = service.qualify_candidate(_candidate(season=7), _watch())
    inferred_watch = _watch()
    inferred_watch["source_season_rules"] = {
        "inferred": {"enabled": True, "season_link": True, "text_patterns": ["portrait"]}
    }
    inferred = service.qualify_candidate(_candidate(season=None), inferred_watch)

    assert (confirmed.status, confirmed.season_link) == ("confirmed", True)
    assert (inferred.status, inferred.season_link) == ("inferred", True)
    assert (unresolved.status, unresolved.season_link) == ("unresolved", False)
    assert unresolved.evidence["kind"] == "insufficient_season_evidence"


def test_stale_worker_is_fenced_before_database_commit() -> None:
    repo, r2, candidate = _Repository(), _R2(), _candidate()
    watcher, downloads = _service(repo, r2, _result(candidate), commit=lambda **_kwargs: None)

    result = watcher.run(_watch(), lease_owner="worker", lease_fence=9)

    assert result.status == "fenced"
    assert downloads == ["image-1"]
    assert repo.finished == []


def test_cap_continuation_keeps_prior_watermark_and_is_durable() -> None:
    repo, r2 = _Repository(), _R2()
    prior = {"bravo": {"watermarks": {"created_at": "2026-08-05T00:00:00Z"}}}
    watcher, downloads = _service(repo, r2, _result(complete=False, continuation="resume-token"))

    result = watcher.run(_watch(source_state=prior), lease_owner="worker", lease_fence=4)

    assert result.status == "incomplete"
    assert result.source_state_after == prior
    assert result.continuation == {"bravo": {"token": "resume-token", "reason": "page_cap"}}
    assert repo.finished[-1]["source_state_after"] == prior
    assert downloads == []


def test_baseline_completion_commits_run_marker_and_watermark_before_lease_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, r2 = _Repository(), _R2()
    watcher, _downloads = _service(repo, r2, _result())
    calls: list[tuple[str, list[Any]]] = []

    @contextmanager
    def connection():
        yield object()

    monkeypatch.setattr(service.pg, "db_connection", lambda *, label: connection())
    monkeypatch.setattr(service.pg, "fetch_one", lambda *_args, **_kwargs: {"id": WATCH_ID})

    def execute(sql: str, params: list[Any], *, conn: object) -> list[dict[str, str]]:  # noqa: ARG001
        calls.append((sql, params))
        return [{"id": "ok"}]

    monkeypatch.setattr(service.pg, "execute_returning", execute)
    result = watcher._finish_baseline_atomic(
        run_id="run-1",
        watch_id=WATCH_ID,
        watch=_watch(baseline_completed_at=None),
        baseline_generation_id="baseline-1",
        lease_owner="worker",
        lease_fence=1,
        source_state_after={"bravo": {"watermarks": {"created_at": "2026-08-06T12:00:00Z"}}},
        summary={"observed": 1},
    )

    assert result.status == "completed"
    assert len(calls) == 3
    assert "UPDATE core.show_season_media_watch_runs" in calls[0][0]
    assert "UPDATE core.show_season_media_watch_baseline_generations" in calls[1][0]
    assert "baseline_completed_at" in calls[2][0]
    assert "source_state = %s::jsonb" in calls[2][0]
    assert "lease_owner = NULL" in calls[2][0]


def test_uploaded_object_is_adopted_without_redownload_after_db_failure() -> None:
    repo, r2, candidate = _Repository(), _R2(), _candidate()
    digest = "a" * 64
    key = service.build_revision_r2_key(_watch(), candidate, sha256=digest, content_type="image/jpeg")
    r2.objects[key] = {
        "ContentLength": 123,
        "ContentType": "image/jpeg",
        "Metadata": {"sha256": digest},
        "ETag": '"adopted"',
    }
    fingerprint = service.source_fingerprint(candidate)
    repo.observations[(WATCH_ID, "bravo", "image-1")] = {
        "source_fingerprint": fingerprint,
        "acquisition_state": "r2_uploaded",
        "revalidate_after": None,
        "metadata": {"pending_acquisition": {"sha256": digest, "key": key, "content_type": "image/jpeg"}},
    }
    watcher, downloads = _service(repo, r2, _result(candidate))

    result = watcher.run(_watch(), lease_owner="worker", lease_fence=5)

    assert result.summary["adopted"] == 1
    assert downloads == []
    assert r2.puts == 0


def test_secure_downloader_revalidates_a_private_redirect(monkeypatch) -> None:
    class Redirect:
        status_code = 302
        headers = {"Location": "https://127.0.0.1/private"}

        def close(self) -> None:
            return None

    monkeypatch.setattr(service.s3_mirror, "_public_media_url_error", lambda _url: None)
    with pytest.raises(service.UnsafeDownloadError, match="literal IP"):
        service.secure_download_candidate(_candidate(), request_get=lambda *_args, **_kwargs: Redirect())


def test_r2_key_is_content_addressed_and_does_not_include_source_path() -> None:
    candidate = _candidate(source_asset_id="A /../ asset")
    candidate["filename"] = "../../secret.jpg"
    key = service.build_revision_r2_key(_watch(), candidate, sha256="b" * 64, content_type="image/jpeg")

    assert key.startswith("shows/the-watch/season-7/bravo/a---asset/")
    assert ".." not in key.split("/")
    assert key.endswith("bbbbbbbbbbbbbbbb-secret.jpg")
