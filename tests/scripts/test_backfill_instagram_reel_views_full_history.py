from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.socials.backfill_instagram_reel_views_full_history as mod


def test_collect_run_diagnostics_paginates_and_counts_skipped_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, int]] = []

    def _fake_list_jobs(season_id: str, *, run_id: str, limit: int, offset: int = 0, **_kwargs):
        calls.append((limit, offset))
        assert season_id == "season-1"
        assert run_id == "run-1"
        if offset == 0:
            page = [
                {
                    "status": "completed",
                    "metadata": {
                        "retrieval_meta": {
                            "scrape_counters": {"posts": 0},
                            "details_refresh_views_updated": 0,
                            "details_refresh_views_preserved_missing": 0,
                            "details_refresh_errors": 0,
                            "details_refresh_detail_fetch_skipped_limit": 0,
                        }
                    },
                }
                for _ in range(mod.JOB_PAGE_SIZE - 1)
            ]
            page.append(
                {
                    "status": "completed",
                    "metadata": {
                        "retrieval_meta": {
                            "scrape_counters": {"posts": 4},
                            "details_refresh_views_updated": 2,
                            "details_refresh_views_preserved_missing": 1,
                            "details_refresh_errors": 1,
                            "details_refresh_detail_fetch_skipped_limit": 3,
                            "details_refresh_errors_by_reason": {"checkpoint_required": 1},
                        }
                    },
                }
            )
            return page
        if offset == mod.JOB_PAGE_SIZE:
            return [
                {
                    "status": "failed",
                    "metadata": {
                        "retrieval_meta": {
                            "scrape_counters": {"posts": 2},
                            "details_refresh_views_updated": 1,
                            "details_refresh_views_preserved_missing": 0,
                            "details_refresh_errors": 2,
                            "details_refresh_detail_fetch_skipped_limit": 4,
                            "comment_fetch_failures_by_reason": {"fallback_reason": 2},
                        }
                    },
                }
            ]
        return []

    monkeypatch.setattr(mod.social_repo, "list_jobs", _fake_list_jobs)

    payload = mod._collect_run_diagnostics(season_id="season-1", run_id="run-1")

    assert payload["job_count"] == mod.JOB_PAGE_SIZE + 1
    assert payload["posts_scanned"] == 6
    assert payload["views_updated"] == 3
    assert payload["detail_fetch_skipped_limit"] == 7
    assert payload["failures_by_reason"] == {"checkpoint_required": 1, "fallback_reason": 2}
    assert calls == [(mod.JOB_PAGE_SIZE, 0), (mod.JOB_PAGE_SIZE, mod.JOB_PAGE_SIZE)]


def test_main_requires_instagram_auth_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            season_id=[],
            source_scope="bravo",
            wait=False,
            poll_interval_seconds=5.0,
            poll_timeout_seconds=3600,
            initiated_by="script:test",
        ),
    )
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {})

    with pytest.raises(SystemExit, match="missing sessionid cookie"):
        mod.main()


def test_main_requires_positive_detail_fetch_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            season_id=[],
            source_scope="bravo",
            wait=False,
            poll_interval_seconds=5.0,
            poll_timeout_seconds=3600,
            initiated_by="script:test",
        ),
    )
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_MAX_DETAIL_FETCHES", "0")

    with pytest.raises(SystemExit, match="detail-fetch cap is disabled"):
        mod.main()
