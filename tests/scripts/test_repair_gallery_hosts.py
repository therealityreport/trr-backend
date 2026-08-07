from __future__ import annotations

import pytest

import scripts.media.repair_gallery_hosts as mod


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


def test_source_matches_bravo_web_scrape_sources() -> None:
    assert mod._source_matches("web_scrape:bravotv.com", {"bravo"}) is True
    assert mod._source_matches("web_scrape:bravo", {"bravo"}) is True
