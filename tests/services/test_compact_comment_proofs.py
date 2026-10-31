import copy
import json

from trr_backend.ingestion.comment_checkpoints import compact_page_checkpoint, page_progress_matches


def checkpoint(count):
    return {
        "comment_pages": {
            "version": 1,
            "scan_root_evidence_id": "root",
            "page": 2,
            "observed": {str(i): {"text": "x" * 100} for i in range(count)},
            "lanes": {str(i): {"cursor": str(i), "exhausted": True} for i in range(count)},
        }
    }


def test_compact_size_does_not_repeat_accumulated_content():
    small, large = checkpoint(1), checkpoint(1000)
    proof = compact_page_checkpoint(large)
    assert len(json.dumps(proof)) - len(json.dumps(compact_page_checkpoint(small))) < 10
    assert page_progress_matches(proof, large["comment_pages"])
    assert page_progress_matches(large, large["comment_pages"])
    assert "observed" not in proof["comment_pages"]


def test_hash_and_count_tampering_rejected_but_reply_admin_is_independent():
    original = checkpoint(3)
    proof = compact_page_checkpoint(original)
    changed = copy.deepcopy(original["comment_pages"])
    changed["reply_access"] = {"status": "blocked"}
    assert page_progress_matches(proof, changed)
    changed["observed"]["0"]["text"] = "changed"
    assert not page_progress_matches(proof, changed)
    proof["comment_pages"]["observed_count"] = 999
    assert not page_progress_matches(proof, original["comment_pages"])
