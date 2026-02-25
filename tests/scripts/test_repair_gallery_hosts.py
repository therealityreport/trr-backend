from __future__ import annotations

import importlib
import json

import pytest

mod = importlib.import_module("scripts.media.repair_gallery_hosts")


def _candidate(
    *,
    kind: mod.CandidateKind,
    row_id: str,
    hosted_url: str | None,
    source_url: str | None,
) -> mod.RepairCandidate:
    if kind == "media_link_asset":
        row = {
            "id": row_id,
            "source": "imdb",
            "source_url": source_url,
            "hosted_url": hosted_url,
            "metadata": {},
        }
        return mod.RepairCandidate(
            kind=kind,
            person_id="person-1",
            source="imdb",
            source_url=source_url,
            hosted_url=hosted_url,
            row_id=row_id,
            row=row,
            metadata={},
            link_id=f"link-{row_id}",
            link_context={},
            source_page_url=None,
        )
    row = {
        "id": row_id,
        "person_id": "person-1",
        "source": "imdb",
        "url": source_url,
        "hosted_url": hosted_url,
        "metadata": {},
    }
    return mod.RepairCandidate(
        kind=kind,
        person_id="person-1",
        source="imdb",
        source_url=source_url,
        hosted_url=hosted_url,
        row_id=row_id,
        row=row,
        metadata={},
        link_id=None,
        link_context=None,
        source_page_url=None,
    )


def test_repair_gallery_hosts_dry_run_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ok_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-ok",
        hosted_url="https://cdn.example.com/ok.jpg",
        source_url="https://source.example.com/ok.jpg",
    )
    repaired_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-repair",
        hosted_url="https://cdn.example.com/fail.jpg",
        source_url="https://source.example.com/repair.jpg",
    )
    broken_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-broken",
        hosted_url="https://cdn.example.com/broken.jpg",
        source_url="https://source.example.com/broken.jpg",
    )
    monkeypatch.setattr(
        mod,
        "_collect_candidates",
        lambda *_args, **_kwargs: [ok_candidate, repaired_candidate, broken_candidate],
    )

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        if url and url.endswith("/ok.jpg"):
            return mod.ReachabilityProbeResult(ok=True, reason="http_200", attempts=1, transient_failure=False)
        if url and url.endswith("/repair.jpg"):
            return mod.ReachabilityProbeResult(ok=True, reason="http_200", attempts=1, transient_failure=False)
        return mod.ReachabilityProbeResult(ok=False, reason="http_403", attempts=1, transient_failure=False)

    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(
        mod,
        "_mark_candidate_broken",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()),
    )

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=False,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
    )

    assert report["summary"]["scanned"] == 3
    assert report["summary"]["ok"] == 1
    assert report["summary"]["repaired"] == 1
    assert report["summary"]["broken_unreachable"] == 1
    assert report["summary"]["error"] == 0
    assert report["repaired_ids"] == ["cast-repair"]
    assert report["broken_ids"] == ["asset-broken"]


def test_repair_gallery_hosts_apply_mutates_expected_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repaired_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-repair",
        hosted_url="https://cdn.example.com/fail.jpg",
        source_url="https://source.example.com/repair.jpg",
    )
    broken_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-broken",
        hosted_url="https://cdn.example.com/broken.jpg",
        source_url="https://source.example.com/broken.jpg",
    )
    monkeypatch.setattr(
        mod,
        "_collect_candidates",
        lambda *_args, **_kwargs: [repaired_candidate, broken_candidate],
    )

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        if url and url.endswith("/repair.jpg"):
            return mod.ReachabilityProbeResult(ok=True, reason="http_200", attempts=1, transient_failure=False)
        return mod.ReachabilityProbeResult(ok=False, reason="http_403", attempts=1, transient_failure=False)

    repaired_calls: list[str] = []
    broken_calls: list[str] = []

    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)
    monkeypatch.setattr(
        mod,
        "_repair_candidate",
        lambda _db, candidate, verbose: repaired_calls.append(candidate.row_id),
    )
    monkeypatch.setattr(
        mod,
        "_mark_candidate_broken",
        lambda _db, candidate, reason: broken_calls.append(candidate.row_id),
    )

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=True,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
    )

    assert repaired_calls == ["cast-repair"]
    assert broken_calls == ["asset-broken"]
    assert report["summary"]["repaired"] == 1
    assert report["summary"]["broken_unreachable"] == 1


def test_repair_gallery_hosts_apply_failure_counts_error_not_repaired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repaired_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-fails",
        hosted_url="https://cdn.example.com/fail.jpg",
        source_url="https://source.example.com/repair.jpg",
    )
    monkeypatch.setattr(mod, "_collect_candidates", lambda *_args, **_kwargs: [repaired_candidate])

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        if url and url.endswith("/repair.jpg"):
            return mod.ReachabilityProbeResult(ok=True, reason="http_200", attempts=1, transient_failure=False)
        return mod.ReachabilityProbeResult(ok=False, reason="http_403", attempts=1, transient_failure=False)

    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=True,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
    )

    assert report["summary"]["repaired"] == 0
    assert report["summary"]["error"] == 1
    assert report["repaired_ids"] == []
    assert report["error_ids"] == ["cast-fails"]
    assert not any(item["status"] == "repaired" for item in report["details"])
    detail = report["details"][0]
    assert detail["status"] == "error"
    assert detail["operation_stage"] == "apply_repair"
    assert detail["exception_type"] == "RuntimeError"
    assert detail["source_probe_reason"] == "http_200"


def test_repair_gallery_hosts_fail_fast_aborts_after_first_apply_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = [
        _candidate(
            kind="cast_photo",
            row_id="cast-fail-1",
            hosted_url="https://cdn.example.com/fail-1.jpg",
            source_url="https://source.example.com/repair-1.jpg",
        ),
        _candidate(
            kind="cast_photo",
            row_id="cast-fail-2",
            hosted_url="https://cdn.example.com/fail-2.jpg",
            source_url="https://source.example.com/repair-2.jpg",
        ),
    ]
    monkeypatch.setattr(mod, "_collect_candidates", lambda *_args, **_kwargs: candidates)

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        if url and "repair-" in url:
            return mod.ReachabilityProbeResult(ok=True, reason="http_200", attempts=1, transient_failure=False)
        return mod.ReachabilityProbeResult(ok=False, reason="http_403", attempts=1, transient_failure=False)

    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=True,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
        fail_fast_on_apply_error=True,
    )

    assert report["summary"]["scanned"] == 2
    assert report["run_meta"]["processed"] == 1
    assert report["run_meta"]["aborted_early"] is True
    assert report["error_ids"] == ["cast-fail-1"]


def test_mark_candidate_broken_updates_media_link_context_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-1",
        hosted_url="https://cdn.example.com/a.jpg",
        source_url="https://source.example.com/a.jpg",
    )
    context_updates: list[tuple[str, dict[str, object]]] = []
    cast_updates: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        mod,
        "_update_media_link_context",
        lambda _db, link_id, context: context_updates.append((link_id, context)),
    )
    monkeypatch.setattr(
        mod,
        "_update_cast_photo",
        lambda _db, photo_id, patch: cast_updates.append((photo_id, patch)),
    )

    mod._mark_candidate_broken(object(), candidate, reason="hosted=http_403;source=http_404")

    assert len(context_updates) == 1
    assert context_updates[0][0] == "link-asset-1"
    assert context_updates[0][1]["gallery_status"] == "broken_unreachable"
    assert cast_updates == []


def test_repair_candidate_regenerates_base_and_crop_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    media_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-1",
        hosted_url="https://cdn.example.com/asset.jpg",
        source_url="https://source.example.com/asset.jpg",
    )
    media_candidate = mod.RepairCandidate(
        **{
            **media_candidate.__dict__,
            "metadata": {},
            "link_context": {"thumbnail_crop": {"x": 50, "y": 30, "zoom": 1.4, "mode": "auto"}},
        }
    )
    cast_candidate = _candidate(
        kind="cast_photo",
        row_id="cast-1",
        hosted_url="https://cdn.example.com/cast.jpg",
        source_url="https://source.example.com/cast.jpg",
    )
    cast_candidate = mod.RepairCandidate(
        **{
            **cast_candidate.__dict__,
            "metadata": {"thumbnail_crop": {"x": 52, "y": 40, "zoom": 1.2, "mode": "manual"}},
        }
    )

    media_updates: list[str] = []
    cast_updates: list[str] = []
    media_variant_calls: list[tuple[str, bool]] = []
    cast_variant_calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(mod, "mirror_media_asset_row", lambda *_args, **_kwargs: {"hosted_url": "x"})
    monkeypatch.setattr(mod, "mirror_cast_photo_row", lambda *_args, **_kwargs: {"hosted_url": "x"})
    monkeypatch.setattr(mod, "_update_media_asset", lambda _db, asset_id, patch: media_updates.append(asset_id))
    monkeypatch.setattr(mod, "_update_cast_photo", lambda _db, photo_id, patch: cast_updates.append(photo_id))
    monkeypatch.setattr(
        mod,
        "generate_media_asset_variants",
        lambda _db, *, asset_id, crop, force: media_variant_calls.append((asset_id, crop is not None)),
    )
    monkeypatch.setattr(
        mod,
        "generate_cast_photo_variants",
        lambda _db, *, photo_id, crop, force: cast_variant_calls.append((photo_id, crop is not None)),
    )

    mod._repair_candidate(object(), media_candidate, verbose=False)
    mod._repair_candidate(object(), cast_candidate, verbose=False)

    assert media_updates == ["asset-1"]
    assert cast_updates == ["cast-1"]
    assert media_variant_calls == [("asset-1", False), ("asset-1", True)]
    assert cast_variant_calls == [("cast-1", False), ("cast-1", True)]


def test_repair_candidate_relinks_media_link_on_source_sha_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    media_candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-dup",
        hosted_url=None,
        source_url="https://source.example.com/asset.jpg",
    )
    media_candidate = mod.RepairCandidate(
        **{
            **media_candidate.__dict__,
            "source": "fandom",
            "metadata": {},
            "link_context": {"thumbnail_crop": {"x": 50, "y": 30, "zoom": 1.4, "mode": "auto"}},
        }
    )

    monkeypatch.setattr(
        mod,
        "mirror_media_asset_row",
        lambda *_args, **_kwargs: {
            "source": "fandom",
            "hosted_sha256": "abc123",
            "hosted_key": "media/ab/abc123.jpg",
            "hosted_url": "https://cdn.example.com/media/ab/abc123.jpg",
        },
    )
    monkeypatch.setattr(
        mod,
        "_update_media_asset",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError('duplicate key value violates unique constraint "media_assets_source_hosted_sha_uq"')
        ),
    )
    monkeypatch.setattr(
        mod,
        "_find_existing_media_asset_id_by_source_sha",
        lambda *_args, **_kwargs: "asset-canonical",
    )

    relink_calls: list[tuple[str, str]] = []
    variant_calls: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        mod,
        "_update_media_link_asset",
        lambda _db, link_id, asset_id: relink_calls.append((link_id, asset_id)),
    )
    monkeypatch.setattr(
        mod,
        "generate_media_asset_variants",
        lambda _db, *, asset_id, crop, force: variant_calls.append((asset_id, crop is not None)),
    )

    mod._repair_candidate(object(), media_candidate, verbose=False)

    assert relink_calls == [("link-asset-dup", "asset-canonical")]
    assert variant_calls == [("asset-canonical", False), ("asset-canonical", True)]


def test_generate_media_asset_variants_resilient_remirrors_on_missing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generate_calls: list[str] = []
    update_calls: list[str] = []

    def fake_generate_media_asset_variants(_db, *, asset_id: str, crop, force: bool) -> None:
        del crop, force
        generate_calls.append(asset_id)
        if len(generate_calls) == 1:
            raise RuntimeError("An error occurred (NoSuchKey) when calling the GetObject operation")

    monkeypatch.setattr(mod, "generate_media_asset_variants", fake_generate_media_asset_variants)
    monkeypatch.setattr(
        mod,
        "_get_media_asset_row_for_repair",
        lambda _db, _asset_id: {"id": "asset-1", "source": "fandom", "source_url": "https://source.example.com/a.jpg"},
    )
    monkeypatch.setattr(mod, "mirror_media_asset_row", lambda *_args, **_kwargs: {"hosted_url": "https://cdn.example.com/a.jpg"})
    monkeypatch.setattr(
        mod,
        "_update_media_asset",
        lambda _db, asset_id, patch: update_calls.append(f"{asset_id}:{patch.get('hosted_url')}"),
    )

    mod._generate_media_asset_variants_resilient(
        object(),
        asset_id="asset-1",
        crop=None,
        force=True,
        verbose=False,
    )

    assert generate_calls == ["asset-1", "asset-1"]
    assert update_calls == ["asset-1:https://cdn.example.com/a.jpg"]


def test_repair_gallery_hosts_transient_indeterminate_becomes_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-transient",
        hosted_url="https://cdn.example.com/transient.jpg",
        source_url="https://source.example.com/transient.jpg",
    )
    monkeypatch.setattr(mod, "_collect_candidates", lambda *_args, **_kwargs: [candidate])

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        if url and "cdn.example.com" in url:
            return mod.ReachabilityProbeResult(
                ok=False,
                reason="http_503",
                attempts=2,
                transient_failure=True,
            )
        return mod.ReachabilityProbeResult(
            ok=False,
            reason="http_404",
            attempts=1,
            transient_failure=False,
        )

    marked_broken: list[str] = []
    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)
    monkeypatch.setattr(
        mod,
        "_mark_candidate_broken",
        lambda _db, broken_candidate, reason: marked_broken.append(f"{broken_candidate.row_id}:{reason}"),
    )
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: None)

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=True,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
    )

    assert report["summary"]["error"] == 1
    assert report["summary"]["broken_unreachable"] == 0
    assert report["error_ids"] == ["asset-transient"]
    assert marked_broken == []


def test_probe_url_reachability_retries_transient_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_check_url_reachability(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None = None,
    ) -> tuple[bool, str]:
        del source, timeout, source_page_url
        assert url == "https://example.com/image.jpg"
        calls["count"] += 1
        if calls["count"] < 3:
            return False, "http_503"
        return True, "http_200"

    monkeypatch.setattr(mod, "_check_url_reachability", fake_check_url_reachability)

    result = mod._probe_url_reachability(
        url="https://example.com/image.jpg",
        source="imdb",
        timeout=1.0,
        source_page_url=None,
        retry_attempts=3,
        retry_backoff_ms=0,
    )

    assert calls["count"] == 3
    assert result.ok is True
    assert result.reason == "http_200"
    assert result.attempts == 3
    assert result.transient_failure is False


def test_repair_gallery_hosts_confirmation_pass_prevents_broken_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate(
        kind="media_link_asset",
        row_id="asset-confirm",
        hosted_url="https://cdn.example.com/confirm.jpg",
        source_url="https://source.example.com/confirm.jpg",
    )
    monkeypatch.setattr(mod, "_collect_candidates", lambda *_args, **_kwargs: [candidate])
    source_probe_calls = {"count": 0}

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        if url and "cdn.example.com" in url:
            return mod.ReachabilityProbeResult(
                ok=False,
                reason="http_403",
                attempts=1,
                transient_failure=False,
            )
        source_probe_calls["count"] += 1
        if source_probe_calls["count"] == 1:
            return mod.ReachabilityProbeResult(
                ok=False,
                reason="http_404",
                attempts=1,
                transient_failure=False,
            )
        return mod.ReachabilityProbeResult(
            ok=True,
            reason="http_200",
            attempts=1,
            transient_failure=False,
        )

    marked_broken: list[str] = []
    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)
    monkeypatch.setattr(mod, "_repair_candidate", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        mod,
        "_mark_candidate_broken",
        lambda _db, broken_candidate, reason: marked_broken.append(f"{broken_candidate.row_id}:{reason}"),
    )

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=False,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
    )

    assert report["summary"]["repaired"] == 1
    assert report["summary"]["broken_unreachable"] == 0
    assert report["repaired_ids"] == ["asset-confirm"]
    assert marked_broken == []


def test_parse_args_includes_retry_and_confirm_defaults() -> None:
    args = mod._parse_args([])
    assert args.retry_attempts == 2
    assert args.retry_backoff_ms == 500
    assert args.confirm_unreachable_pass is True
    assert args.progress_every == 100
    assert args.checkpoint_every == 250
    assert args.resume_from_checkpoint is False
    assert args.force_flush_progress is True
    assert args.fail_fast_on_apply_error is False


def test_repair_gallery_hosts_emits_heartbeat_and_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: pytest.TempPathFactory,
) -> None:
    candidates = [
        _candidate(
            kind="media_link_asset",
            row_id="asset-1",
            hosted_url="https://cdn.example.com/ok-1.jpg",
            source_url="https://source.example.com/ok-1.jpg",
        ),
        _candidate(
            kind="cast_photo",
            row_id="cast-2",
            hosted_url="https://cdn.example.com/ok-2.jpg",
            source_url="https://source.example.com/ok-2.jpg",
        ),
    ]
    monkeypatch.setattr(mod, "_collect_candidates", lambda *_args, **_kwargs: candidates)
    monkeypatch.setattr(
        mod,
        "_probe_url_reachability",
        lambda **_kwargs: mod.ReachabilityProbeResult(
            ok=True,
            reason="http_200",
            attempts=1,
            transient_failure=False,
        ),
    )

    checkpoint_file = tmp_path / "repair-checkpoint.json"
    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=False,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
        progress_every=1,
        checkpoint_file=checkpoint_file,
        checkpoint_every=1,
        resume_from_index=0,
        force_flush_progress=True,
    )

    out = capsys.readouterr().out
    assert "[heartbeat]" in out
    assert checkpoint_file.exists()
    checkpoint = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    assert checkpoint["mode"] == "completed"
    assert checkpoint["last_index"] == 1
    assert checkpoint["summary"]["ok"] == 2
    assert report["summary"]["scanned"] == 2


def test_load_resume_state_uses_checkpoint_last_index(
    tmp_path: pytest.TempPathFactory,
) -> None:
    checkpoint_file = tmp_path / "resume.json"
    checkpoint_file.write_text(
        json.dumps(
            {
                "version": 1,
                "mode": "running",
                "last_index": 4,
            }
        ),
        encoding="utf-8",
    )
    resume = mod._load_resume_state(checkpoint_file)
    assert resume.start_index == 5


def test_repair_gallery_hosts_resume_from_index_skips_prior_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = [
        _candidate(
            kind="media_link_asset",
            row_id=f"asset-{idx}",
            hosted_url=f"https://cdn.example.com/ok-{idx}.jpg",
            source_url=f"https://source.example.com/ok-{idx}.jpg",
        )
        for idx in range(3)
    ]
    monkeypatch.setattr(mod, "_collect_candidates", lambda *_args, **_kwargs: candidates)

    probed_urls: list[str | None] = []

    def fake_probe(
        *,
        url: str | None,
        source: str,
        timeout: float,
        source_page_url: str | None,
        retry_attempts: int,
        retry_backoff_ms: int,
    ) -> mod.ReachabilityProbeResult:
        del source, timeout, source_page_url, retry_attempts, retry_backoff_ms
        probed_urls.append(url)
        return mod.ReachabilityProbeResult(
            ok=True,
            reason="http_200",
            attempts=1,
            transient_failure=False,
        )

    monkeypatch.setattr(mod, "_probe_url_reachability", fake_probe)

    report = mod.repair_gallery_hosts(
        object(),
        allowed_sources={"imdb"},
        person_ids=[],
        show_ids=[],
        limit=None,
        apply_updates=False,
        timeout=1.0,
        retry_attempts=2,
        retry_backoff_ms=500,
        confirm_unreachable_pass=True,
        verbose=False,
        resume_from_index=2,
    )

    assert report["summary"]["scanned"] == 1
    assert probed_urls == ["https://cdn.example.com/ok-2.jpg"]
