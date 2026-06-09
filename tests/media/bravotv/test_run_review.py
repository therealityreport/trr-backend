from __future__ import annotations

from pathlib import Path

from trr_backend.media.bravotv.run_review import build_run_review_from_dir


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "media" / "bravotv_image_run"


def test_build_run_review_from_fixture_artifacts() -> None:
    report = build_run_review_from_dir(FIXTURE_ROOT)

    assert report["summary"]["total_merged_records"] == 2
    assert report["source_counts"] == {"bravo": 1, "getty": 1, "nbcumv": 1}
    assert report["bravo_gallery_source_breakdown"]["total_raw_bravo_rows"] == 1
    assert report["bravo_gallery_source_breakdown"]["by_extraction_method"] == {"jsonapi_media_item": 1}
    assert report["bridge_strategy_counts"]["A_nup_filename"] == 1
    assert report["review_reason_counts"]["ambiguous_people_match"] == 1
    assert report["review_reason_counts"]["replacement_pending"] == 1
    assert report["review_reason_counts"]["source_mismatch"] == 1
    assert report["entity_link_counts"] == {"missing": 1, "person": 1, "season": 1, "show": 1}
    assert report["quality_buckets"]["low_resolution"] == 1
    assert report["quality_buckets"]["usable_dimensions"] == 1
    assert report["replacement_pending"][0]["group_id"] == "bridge-00002"
    assert report["recommended_next_actions"]
