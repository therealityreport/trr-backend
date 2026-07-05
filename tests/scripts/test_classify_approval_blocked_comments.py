from __future__ import annotations

import json

import scripts.socials.instagram.classify_approval_blocked_comments as mod


def test_extract_approval_targets_uses_runtime_metadata_once() -> None:
    metadata = {
        "runtime_metadata": {
            "incomplete_target_source_ids": ["SHORT1", "SHORT2"],
            "zero_comment_incomplete_target_source_ids": ["SHORT2", "ZERO1"],
        },
        "retry_rebalance": {"remaining_target_source_ids": ["SHORT1", "RETRY1"]},
    }

    targets = mod._extract_approval_target_source_ids(metadata, {})

    assert targets == ["SHORT1", "SHORT2", "ZERO1", "RETRY1"]


def test_build_payload_dry_run_reports_eligible_gap_and_skips_high_volume(monkeypatch) -> None:
    monkeypatch.setattr(
        mod,
        "_fetch_approval_jobs",
        lambda **_kwargs: [
            {
                "job_id": "job-1",
                "config": {},
                "metadata": {
                    "runtime_metadata": {
                        "incomplete_target_source_ids": ["SHORT1", "BIG1", "MISSING1"],
                        "incomplete_fetch_reasons": {
                            "SHORT1": "public_comments_partial_requires_approval",
                            "BIG1": "public_comments_partial_requires_approval",
                        },
                    }
                },
            }
        ],
    )
    monkeypatch.setattr(
        mod,
        "_fetch_target_post_rows",
        lambda **_kwargs: {
            "SHORT1": {
                "post_id": "post-1",
                "reported_comments": 12,
                "stored_total_comments": 5,
                "existing_missing_comments": 1,
                "facebook_comment_count": 0,
            },
            "BIG1": {
                "post_id": "post-2",
                "reported_comments": 100,
                "stored_total_comments": 1,
                "existing_missing_comments": 0,
                "facebook_comment_count": 0,
            },
        },
    )

    payload = mod._build_payload(
        run_id="run-1",
        account_handle="bravotv",
        target_source_ids=[],
        include_non_failed_jobs=False,
        job_limit=None,
        max_reported_comments=99,
        max_comments_per_post=0,
        apply=False,
    )

    assert payload["mode"] == "dry_run"
    assert payload["totals"] == {
        "candidate_targets": 3,
        "eligible_targets": 1,
        "would_insert_missing_comments": 6,
        "inserted_missing_comments": 0,
        "skipped_targets": 2,
    }
    rows = {row["shortcode"]: row for row in payload["targets"]}
    assert rows["SHORT1"]["eligible"] is True
    assert rows["SHORT1"]["would_insert_missing_comments"] == 6
    assert rows["BIG1"]["skip_reason"] == "reported_comments_above_terminal_threshold"
    assert rows["MISSING1"]["skip_reason"] == "post_not_found"


def test_build_payload_apply_classifies_only_eligible_targets(monkeypatch) -> None:
    monkeypatch.setattr(
        mod,
        "_fetch_approval_jobs",
        lambda **_kwargs: [
            {
                "job_id": "job-1",
                "config": {},
                "metadata": {"runtime_metadata": {"incomplete_target_source_ids": ["SHORT1", "DONE1"]}},
            }
        ],
    )
    monkeypatch.setattr(
        mod,
        "_fetch_target_post_rows",
        lambda **_kwargs: {
            "SHORT1": {
                "post_id": "post-1",
                "reported_comments": 12,
                "stored_total_comments": 5,
                "existing_missing_comments": 1,
                "facebook_comment_count": 0,
            },
            "DONE1": {
                "post_id": "post-2",
                "reported_comments": 4,
                "stored_total_comments": 4,
                "existing_missing_comments": 0,
                "facebook_comment_count": 0,
            },
        },
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        mod,
        "_classify_unavailable_instagram_comment_gap",
        lambda **kwargs: calls.append(kwargs) or 6,
    )

    payload = mod._build_payload(
        run_id="run-1",
        account_handle="bravotv",
        target_source_ids=[],
        include_non_failed_jobs=False,
        job_limit=None,
        max_reported_comments=99,
        max_comments_per_post=0,
        apply=True,
        conn=object(),
    )

    assert payload["totals"]["inserted_missing_comments"] == 6
    assert len(calls) == 1
    assert calls[0]["post_id"] == "post-1"
    assert calls[0]["job_id"] == "job-1"
    assert calls[0]["reason"] == mod.APPROVAL_BLOCKED_MISSING_CLASSIFICATION_REASON


def test_main_apply_refuses_without_confirmation(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: (_ for _ in ()).throw(AssertionError("loaded env")))

    assert mod.main(["--run-id", "run-1", "--account", "bravotv", "--apply"]) == 2

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "refused"
    assert payload["mode"] == "dry_run"
    assert "missing --confirm-apply" in payload["refusal_reasons"]
    assert "missing --confirm-run-id" in payload["refusal_reasons"]
