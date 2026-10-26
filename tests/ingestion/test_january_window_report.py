"""Recovery reporting separates fresh observations, replay and existing IDs."""

from io import BytesIO

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


def comment(native, parent=None, *, missing=False):
    return {
        "shortcode": "post",
        "comment_id": native,
        "parent_comment_external_id": parent,
        "linked_parent": parent,
        "is_missing": missing,
        "hosted_media_urls": [],
        "hosted_avatar": None,
    }


def snapshot(comments):
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


def test_media_counts_logical_slots_separately_from_full_object_reads(monkeypatch):
    base = "https://storage.example"
    before, after = snapshot([]), snapshot([])
    after["comments"] = [
        dict(comment("a"), hosted_media_urls=[base + "/object"]),
        dict(comment("b"), hosted_media_urls=[base + "/object"]),
    ]
    response = BytesIO(b"preserved bytes")
    response.url = base + "/object"
    response.headers = {"Content-Length": "15"}
    monkeypatch.setattr("urllib.request.urlopen", lambda *_a, **_k: response)
    result = operator.verify_new_media(before, after, [base])
    assert result["new_or_changed_logical_slots"] == 2
    assert result["new_distinct_hosted_urls"] == 1
    assert len(result["full_objects_read"]) == 1 and result["full_objects_read"][0]["bytes"] == 15
    assert result["unverified"] == []
