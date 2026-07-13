from __future__ import annotations

from typing import Any

import pytest

import trr_backend.socials.pipelines.comments.instagram as pipeline

RUN_ID = "11111111-1111-1111-1111-111111111111"


class _FakeGapRepairPg:
    def __init__(self, *, run_config: dict[str, Any], job_rows: list[dict[str, Any]]) -> None:
        self.run_config = run_config
        self.job_rows = job_rows

    def fetch_one(self, _sql: str, _params: list[Any]) -> dict[str, Any]:
        return {
            "run_id": RUN_ID,
            "source_scope": "network",
            "initiated_by": "test",
            "config": self.run_config,
            "run_metadata": {},
        }

    def fetch_all(self, _sql: str, _params: list[Any]) -> list[dict[str, Any]]:
        return list(self.job_rows)


def test_comments_target_gap_repair_preserves_incomplete_filter_and_date_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selector_calls: list[dict[str, Any]] = []
    created_configs: list[dict[str, Any]] = []

    monkeypatch.setattr(
        pipeline,
        "pg",
        _FakeGapRepairPg(
            run_config={
                "stage": pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                "account": "BravoTV",
                "mode": "profile",
                "refresh_policy": "stale_or_missing",
                "target_filter": "incomplete",
                "incomplete_fill": True,
                "max_posts": 5,
                "comments_shard_count": 1,
                "date_start": "2025-01-01T00:00:00+00:00",
                "date_end": "2025-02-01T00:00:00+00:00",
            },
            job_rows=[
                {
                    "status": "running",
                    "config": {
                        "stage": pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                        "target_source_ids": ["already-assigned"],
                    },
                }
            ],
        ),
    )

    def fake_incomplete_selector(account_handle: str, **kwargs: Any) -> list[str]:
        selector_calls.append({"account_handle": account_handle, **kwargs})
        return ["already-assigned", "missing-target"]

    monkeypatch.setattr(
        pipeline,
        "_instagram_social_account_incomplete_comment_target_shortcodes",
        fake_incomplete_selector,
    )
    monkeypatch.setattr(
        pipeline,
        "_instagram_social_account_comment_target_shortcodes",
        lambda *_args, **_kwargs: pytest.fail("generic selector should not run for target_filter=incomplete"),
    )

    def fake_create_job(_context: Any, **kwargs: Any) -> str:
        created_configs.append(kwargs["config"])
        return "repair-job-1"

    monkeypatch.setattr(pipeline, "_create_job", fake_create_job)

    result = pipeline.repair_instagram_comments_scrape_run_target_gaps(
        run_id=RUN_ID,
        dispatch_immediately=False,
    )

    assert result["created_job_ids"] == ["repair-job-1"]
    assert selector_calls == [
        {
            "account_handle": "bravotv",
            "limit": 5,
            "date_start": "2025-01-01T00:00:00+00:00",
            "date_end": "2025-02-01T00:00:00+00:00",
        }
    ]
    assert created_configs[0]["target_source_ids"] == ["missing-target"]
    assert created_configs[0]["target_filter"] == "incomplete"
    assert created_configs[0]["date_start"] == "2025-01-01T00:00:00+00:00"
    assert created_configs[0]["date_end"] == "2025-02-01T00:00:00+00:00"


def test_comments_target_gap_repair_keeps_generic_selector_when_filter_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selector_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        pipeline,
        "pg",
        _FakeGapRepairPg(
            run_config={
                "stage": pipeline.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                "account": "bravotv",
                "mode": "profile",
                "refresh_policy": "all_saved_posts",
                "max_posts": 3,
                "comments_shard_count": 1,
            },
            job_rows=[],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_instagram_social_account_incomplete_comment_target_shortcodes",
        lambda *_args, **_kwargs: pytest.fail("incomplete selector should not run without target_filter"),
    )

    def fake_generic_selector(account_handle: str, **kwargs: Any) -> list[str]:
        selector_calls.append({"account_handle": account_handle, **kwargs})
        return ["missing-target"]

    monkeypatch.setattr(pipeline, "_instagram_social_account_comment_target_shortcodes", fake_generic_selector)
    monkeypatch.setattr(pipeline, "_create_job", lambda _context, **_kwargs: "repair-job-1")

    result = pipeline.repair_instagram_comments_scrape_run_target_gaps(
        run_id=RUN_ID,
        dispatch_immediately=False,
    )

    assert result["created_job_ids"] == ["repair-job-1"]
    assert selector_calls == [
        {
            "account_handle": "bravotv",
            "limit": 3,
            "refresh_policy": "all_saved_posts",
            "date_start": None,
            "date_end": None,
        }
    ]
