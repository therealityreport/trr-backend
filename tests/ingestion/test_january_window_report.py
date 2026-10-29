"""Recovery reporting separates fresh observations, replay and existing IDs."""

import json
from io import BytesIO
from typing import Any

import pytest

from scripts.ingestion import recover_january_window as operator


class Connection:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def close(self):
        pass

    def cursor(self, **_):
        return self

    def execute(self, *_):
        pass

    def fetchall(self):
        return self.rows


def comment(native, parent=None, *, missing=False) -> dict[str, Any]:
    return {
        "shortcode": "post",
        "comment_id": native,
        "parent_comment_external_id": parent,
        "linked_parent": parent,
        "is_missing": missing,
        "hosted_media_urls": [],
        "hosted_avatar": None,
    }


def snapshot(comments) -> dict[str, Any]:
    return {
        "comments": comments,
        "pages": [{"page_id": "old-page", "evidence_id": "old-evidence"}],
        "replays": [],
        "receipts": [],
        "media": [],
        "targets": [
            {
                "stage": "comments",
                "target_key": "post",
                "requests": 1,
                "settlement": {"count_gap": 10, "expected_total": 11},
                "observed_ids": ["old"],
            }
        ],
    }


def test_report_deduplicates_baseline_and_separates_receipt_provenance(monkeypatch):
    before = snapshot([comment("old"), comment("restored", missing=True)])
    after = snapshot(
        [
            comment("old"),
            comment("restored"),
            comment("fresh-root"),
            comment("fresh-reply", "old"),
            comment("replay", "old"),
            comment("both", "old"),
            comment("unknown"),
        ]
    )
    after["replays"] = [
        {
            "receipt_id": "new-replay",
            "evidence_id": "old-evidence",
            "target_key": "post",
            "canonical_identity": {"observed_comment_ids": ["replay", "both"]},
        }
    ]
    rows = [
        {
            "page_id": "new-page",
            "evidence_id": "new-evidence",
            "target_key": "post",
            "canonical_identity": {"observed_comment_ids": ["old", "fresh-root", "fresh-reply", "both"]},
        }
    ]
    monkeypatch.setattr(operator, "connect", lambda: Connection(rows))
    result = operator.report(before, after, {"started_at": "2026-10-26T20:00:00+00:00"})
    assert result["new_canonical_ids"] == 5
    assert result["previously_missing_ids_restored"] == 1
    assert result["per_post"]["post"] == {
        "fresh_source_top_level_comments": 1,
        "fresh_source_replies": 1,
        "retained_replay_replies": 1,
        "both_replies": 1,
        "unattributed_top_level_comments": 1,
    }
    assert result["final_acceptance"] is False
    assert result["target_diagnostics"][0]["diagnostics"] == {"availability": "unknown"}


@pytest.mark.parametrize("structured_binding", [False, True])
def test_media_counts_logical_slots_separately_from_full_object_reads(monkeypatch, structured_binding):
    base = "https://storage.example"
    before, after = snapshot([]), snapshot([])
    after["comments"] = [
        dict(comment("a"), hosted_media_urls=[base + "/object"]),
        dict(comment("b"), hosted_media_urls=[base + "/object"]),
    ]
    response = BytesIO(b"preserved bytes")
    response.url = base + "/object"  # pyright: ignore[reportAttributeAccessIssue]
    response.headers = {"Content-Length": "15"}  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setattr("urllib.request.urlopen", lambda *_a, **_k: response)
    binding = [{"url": base, "verified": True}] if structured_binding else [base]
    result = operator.verify_new_media(before, after, binding)
    assert result["new_or_changed_logical_slots"] == 2
    assert result["new_distinct_hosted_urls"] == 1
    assert len(result["full_objects_read"]) == 1 and result["full_objects_read"][0]["bytes"] == 15
    assert result["unverified"] == []


def test_report_projects_actual_failure_branch_holds_and_lane_proof(monkeypatch):
    before, after = snapshot([]), snapshot([])
    diagnostic = operator.target_diagnostics(
        {
            "last_error_code": "activity_failed",
            "worker_diagnostics": {
                "last_failure": {
                    "code": "technical_TimeoutError",
                    "category": "attempt_deadline",
                    "exception_class": "TimeoutError",
                    "owner_generation": 7,
                    "observed_at": "2026-10-26T20:00:00+00:00",
                },
                "committed_pages_since_failure": 3,
            },
            "comment_pages": {
                "reply_access": {
                    "code": "reply_access_rejected",
                    "attempts": 8,
                    "retry_source": "server_retry_after",
                    "lanes": ["parent"],
                    "next_attempt_at": "2026-10-26T21:00:00+00:00",
                },
                "lanes": {
                    "__top__": {
                        "exhausted": True,
                        "repeated": False,
                        "started_from_root": True,
                        "cursors": ["opaque-one", "opaque-two"],
                    },
                    "parent": {"exhausted": False, "repeated": True, "cursors": ["opaque-child"]},
                    "unknown-proof": {"status": "complete", "outcome": "committed"},
                },
            },
            "public_reply_failures": {
                "parent": {
                    "code": "source_incomplete",
                    "technical_hold": True,
                    "failure_count": 3,
                    "retry_source": "technical_branch_backoff",
                }
            },
        }
    )
    after["targets"][0].update(outcome="unresolved", blocked_reason="reply_access_rejected", diagnostics=diagnostic)
    monkeypatch.setattr(operator, "connect", lambda: Connection([]))
    result = operator.report(before, after, {"started_at": "2026-10-26T20:00:00+00:00"})
    target = result["target_diagnostics"][0]
    assert target["outcome"] == "unresolved" and target["blocked_reason"] == "reply_access_rejected"
    assert diagnostic["last_error_code"] == "activity_failed"
    assert diagnostic["last_failure"]["category"] == "attempt_deadline"
    assert diagnostic["committed_pages_since_failure"] == 3  # Historical failure survives successful pages.
    assert diagnostic["reply_access"]["lane_count"] == 1
    assert diagnostic["reply_access"]["retry_source"] == "server_retry_after"
    assert diagnostic["public_reply_failures"]["technical_hold"] == 1
    proof = diagnostic["lane_proof"]
    assert proof["total"] == 3
    assert proof["exhausted"] == {"true": 1, "false": 1, "unknown": 1}
    assert proof["repeated"] == {"true": 1, "false": 1, "unknown": 1}
    assert proof["started_from_root"] == {"true": 1, "false": 0, "unknown": 2}
    assert proof["cursor_history"] == {"known_lanes": 2, "entries": 3}


def test_diagnostics_distinguishes_old_unknown_metadata_from_recorded_absence():
    assert operator.target_diagnostics(None) == {"availability": "unknown"}
    result = operator.target_diagnostics({})
    assert result["availability"] == "known"
    assert result["last_failure"] is None
    assert result["committed_pages_since_failure"] is None
    assert result["lane_proof"] == {"availability": "absent"}
    assert result["public_reply_failures"] == {"availability": "absent"}
    assert result["reply_access"] == {"availability": "absent"}


def test_diagnostics_bounds_branch_detail_and_omits_requests_cursors_and_secrets():
    secret = "https://storage.example/signed?token=private-credential"
    result = operator.target_diagnostics(
        {
            "last_error_code": secret,
            "worker_diagnostics": {
                "last_failure": {
                    "code": secret,
                    "exception_class": secret,
                    "category": secret,
                    "observed_at": secret,
                    "request": secret,
                }
            },
            "comment_pages": {
                "reply_access": {"code": "reply_access_rejected", "request": secret, "lanes": [secret]},
                "lanes": {secret: {"cursors": [secret], "request": secret, "exhausted": False, "repeated": False}},
            },
            "public_reply_failures": {
                str(i): {
                    "code": "source_incomplete",
                    "technical_hold": True,
                    "request": {"cursor": secret},
                    "failure_count": 3,
                    "next_attempt_at": secret,
                }
                for i in range(105)
            },
        }
    )
    assert "private-credential" not in json.dumps(result)
    assert result["last_error_code"] is None
    assert result["last_failure"]["observed_at"] is None
    assert result["lane_proof"]["cursor_history"]["entries"] == 1
    holds = result["public_reply_failures"]
    assert holds["total"] == holds["technical_hold"] == 105
    assert len(holds["branches"]) == 100 and holds["branches_truncated"] is True


def test_snapshot_projects_checkpoint_before_returning_saved_material(monkeypatch):
    monkeypatch.setattr(operator, "ACCOUNT", "bravotv")
    class SnapshotConnection(Connection):
        def fetchone(self) -> dict[str, Any]:
            return {"account": "bravotv", "intent_hash": operator.INTENT, "execution_placement": "local"}

        def fetchall(self):
            return next(self.rows)

    secret = "https://source.example/?credential=secret"
    connection = SnapshotConnection(
        iter(
            [
                [
                    {
                        "stage": "comments",
                        "target_key": "post",
                        "reporting_checkpoint": {
                            "last_error_code": "reply_access_rejected",
                            "comment_pages": {"lanes": {"__top__": {"exhausted": True, "cursors": [secret]}}},
                            "public_reply_failures": {"parent": {"code": "source_incomplete", "request": secret}},
                        },
                    }
                ],
                [],
                [],
                [],
                [],
                [],
            ]
        )
    )
    monkeypatch.setattr(operator, "connect", lambda: connection)
    result = operator.snapshot()
    assert "credential" not in json.dumps(result)
    target = result["targets"][0]
    assert "reporting_checkpoint" not in target
    assert target["diagnostics"]["last_error_code"] == "reply_access_rejected"
    assert target["diagnostics"]["lane_proof"]["exhausted"]["true"] == 1
